use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::thread::JoinHandle;

use futures::StreamExt;
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures::future::{Either, join_all, select};

use crate::app::{Assignment, Graph};
use crate::messages::{Code, CoordinatorMessage, NodeMessage, RegisterNode, Reply, RequestCode};
use crate::node::NodeId;
use crate::runtime::Runtime;
use crate::serializer::Serializer;
use crate::serializer::bincode::BincodeSerializer;
use crate::time::Time;
use crate::transport::tcp::TcpTransport;
use crate::transport::{Listener, Receiver, Sender, Transport};

/// The transport between nodes and their coordinator; both sides use this one.
pub const TRANSPORT: TcpTransport = TcpTransport;
/// The serializer of the messages between nodes and their coordinator.
pub const SERIALIZER: BincodeSerializer = BincodeSerializer;

pub struct Coordinator<T: Transport, S> {
    listener: T::Listener,
    serializer: S,
    interrupt: Interrupt,
    interrupted: UnboundedReceiver<()>,
}

/// Tells a coordinator that its app was interrupted; any thread may hold one.
#[derive(Clone)]
pub struct Interrupt(UnboundedSender<()>);

impl Interrupt {
    /// Have every node shut down at once.
    pub fn shutdown_now(&self) {
        // A coordinator that has finished has no node left to tell.
        let _ = self.0.unbounded_send(());
    }
}

impl<T, S> Coordinator<T, S>
where
    T: Transport,
    S: Serializer<RequestCode> + Serializer<Code>,
    S: Serializer<RegisterNode> + Serializer<Reply>,
    S: Serializer<NodeMessage> + Serializer<CoordinatorMessage>,
{
    pub async fn new(transport: T, serializer: S) -> Self {
        let (interrupt, interrupted) = unbounded();
        Self {
            listener: transport.bind().await,
            serializer,
            interrupt: Interrupt(interrupt),
            interrupted,
        }
    }

    pub fn endpoint(&self) -> &str {
        self.listener.endpoint()
    }

    /// Return what a launcher calls when its app is interrupted.
    pub fn interrupt(&self) -> Interrupt {
        self.interrupt.clone()
    }

    /// Give every node its code, wire the nodes once all have registered, then serve them until
    /// every node disconnects.
    pub async fn run(
        mut self,
        graph: Graph,
        mut code: HashMap<NodeId, Vec<u8>>,
    ) -> Result<(), String> {
        let mut nodes = BTreeMap::new();
        // Every node first asks for its code.
        for _ in 0..graph.nodes {
            let (mut replier, mut receiver) = self.listener.accept().await;
            let request: RequestCode = self.serializer.deserialize(&receiver.recv().await.unwrap());
            let node_id = request.node_id;
            let reply: Code = code.remove(&node_id);
            replier.send(&self.serializer.serialize(&reply)).await;
            assert!(
                nodes.insert(node_id, (replier, receiver)).is_none(),
                "node {node_id} asked for its code twice"
            );
        }
        // Registrations are read once every node has its code.
        let mut registered = BTreeMap::new();
        for (node_id, (_, receiver)) in &mut nodes {
            let Some(bytes) = receiver.recv().await else {
                break;
            };
            let registration: RegisterNode = self.serializer.deserialize(&bytes);
            registered.insert(*node_id, registration);
        }
        if registered.len() < nodes.len() {
            // A node failed before it registered. The others stay unwired until their launcher,
            // which sees the failure, ends them.
            for (_, receiver) in nodes.values_mut() {
                while receiver.recv().await.is_some() {}
            }
            return Ok(());
        }
        let mut wirings = graph.wire(&registered);
        for (node_id, (replier, _)) in &mut nodes {
            let reply: Reply = match &mut wirings {
                Ok(wirings) => Ok(wirings.remove(node_id).unwrap()),
                Err(error) => Err(error.clone()),
            };
            replier.send(&self.serializer.serialize(&reply)).await;
        }
        wirings?;
        self.coordinate_shutdown(nodes).await;
        Ok(())
    }

    /// When a node asks for a shutdown, give every node the latest tag that any of them offers.
    /// When the app is interrupted, tell every node to shut down at once.
    async fn coordinate_shutdown(mut self, nodes: BTreeMap<usize, (T::Sender, T::Receiver)>) {
        let (events, mut received) = unbounded();
        let mut senders = BTreeMap::new();
        let mut readers = Vec::new();
        for (node_id, (sender, mut receiver)) in nodes {
            senders.insert(node_id, sender);
            let events = events.clone();
            readers.push(async move {
                while let Some(bytes) = receiver.recv().await {
                    events.unbounded_send((node_id, Some(bytes))).unwrap();
                }
                // The node finished or shut down.
                events.unbounded_send((node_id, None)).unwrap();
            });
        }
        drop(events);
        let agree = async {
            // The nodes still to offer a tag, once a shutdown has been requested.
            let mut waiting: Option<BTreeSet<usize>> = None;
            let mut latest = Time::NEVER;
            let mut interrupted = false;
            loop {
                let event = match select(received.next(), self.interrupted.next()).await {
                    Either::Left((event, _)) => event,
                    Either::Right(_) => {
                        // An interrupt replaces a shutdown that is under way, and happens once.
                        if !interrupted {
                            let now = CoordinatorMessage::ShutdownNow;
                            Self::broadcast(&self.serializer, &mut senders, now).await;
                        }
                        (interrupted, waiting) = (true, None);
                        continue;
                    }
                };
                // The nodes have all disconnected once no event is left.
                let Some((node_id, bytes)) = event else {
                    break;
                };
                match bytes.map(|bytes| self.serializer.deserialize(&bytes)) {
                    Some(NodeMessage::RequestShutdown(delay)) => {
                        // One shutdown serves every request.
                        if waiting.is_none() && !interrupted {
                            let ask = CoordinatorMessage::RequestNodeShutdown(delay);
                            Self::broadcast(&self.serializer, &mut senders, ask).await;
                            waiting = Some(senders.keys().copied().collect());
                        }
                        continue;
                    }
                    Some(NodeMessage::ShutdownTime(time)) => latest = latest.max(time),
                    None => drop(senders.remove(&node_id)),
                }
                let Some(waiting) = &mut waiting else {
                    continue;
                };
                if waiting.remove(&node_id) && waiting.is_empty() {
                    let shutdown = CoordinatorMessage::Shutdown(latest);
                    Self::broadcast(&self.serializer, &mut senders, shutdown).await;
                }
            }
        };
        futures::join!(join_all(readers), agree);
    }

    async fn broadcast(
        serializer: &S,
        senders: &mut BTreeMap<usize, T::Sender>,
        message: CoordinatorMessage,
    ) {
        let bytes = serializer.serialize(&message);
        for sender in senders.values_mut() {
            sender.send(&bytes).await;
        }
    }
}

/// A node's connection to its coordinator, which the node opens by asking for its code.
pub struct Link {
    pub(crate) node_id: NodeId,
    /// The code the coordinator held for the node; a node written in Rust has none.
    pub code: Code,
    pub(crate) sender: <TcpTransport as Transport>::Sender,
    pub(crate) receiver: <TcpTransport as Transport>::Receiver,
}

impl Link {
    /// Connect to the coordinator and ask for the node's code.
    pub fn open(assignment: Assignment) -> Self {
        let Assignment {
            node_id,
            coordinator_endpoint,
        } = assignment;
        Runtime::new().block_on(async move {
            let (mut sender, receiver) = TRANSPORT.connect(&coordinator_endpoint).await;
            let request = SERIALIZER.serialize(&RequestCode { node_id });
            let code = SERIALIZER.deserialize(&sender.call(&request).await);
            Self {
                node_id,
                code,
                sender,
                receiver,
            }
        })
    }
}

/// Run a coordinator for the graph on its own thread; return its endpoint, the thread, and
/// what the launcher calls when the app is interrupted.
///
/// `code` holds what each node gets when it asks for its code; a node written in Rust has none.
pub fn spawn(
    graph: Graph,
    code: HashMap<NodeId, Vec<u8>>,
) -> (String, JoinHandle<Result<(), String>>, Interrupt) {
    let (announce, announced) = std::sync::mpsc::channel();
    let coordinator = std::thread::spawn(move || {
        Runtime::new().block_on(async move {
            let coordinator = Coordinator::new(TRANSPORT, SERIALIZER).await;
            let endpoint = coordinator.endpoint().to_string();
            announce.send((endpoint, coordinator.interrupt())).unwrap();
            coordinator.run(graph, code).await
        })
    });
    let (endpoint, interrupt) = announced.recv().unwrap();
    (endpoint, coordinator, interrupt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::graph::tests::{fan_out, graph, registrations};
    use crate::messages::Wiring;

    /// Use a separate thread and runtime to simulate each node process.
    fn spawn_node(
        coordinator_endpoint: String,
        node_id: NodeId,
        registration: RegisterNode,
    ) -> std::thread::JoinHandle<(Code, Reply)> {
        std::thread::spawn(move || {
            let mut link = Link::open(Assignment {
                node_id,
                coordinator_endpoint,
            });
            Runtime::new().block_on(async move {
                let request = SERIALIZER.serialize(&registration);
                let reply = SERIALIZER.deserialize(&link.sender.call(&request).await);
                // Dropping the connection tells the coordinator that the node finished.
                (link.code, reply)
            })
        })
    }

    /// Start the camera, the planner and the logger as nodes 0, 1 and 2.
    fn spawn_nodes(endpoint: &str) -> Vec<std::thread::JoinHandle<(Code, Reply)>> {
        let registrations = registrations().into_iter().enumerate();
        registrations
            .map(|(node_id, registration)| spawn_node(endpoint.to_string(), node_id, registration))
            .collect()
    }

    #[test]
    fn a_node_gets_the_code_that_the_coordinator_holds_for_it() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::new(TRANSPORT, SERIALIZER).await;
            let children = spawn_nodes(coordinator.endpoint());
            let code = HashMap::from([(1, b"planner".to_vec())]);
            assert_eq!(coordinator.run(graph(fan_out()), code).await, Ok(()));
            let codes: Vec<Code> = children
                .into_iter()
                .map(|child| child.join().unwrap().0)
                .collect();
            // The camera and the logger are in Rust and have no code.
            assert_eq!(codes, [None, Some(b"planner".to_vec()), None]);
        });
    }

    #[test]
    fn a_node_that_fails_before_it_registers_leaves_the_others_unwired() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::new(TRANSPORT, SERIALIZER).await;
            let endpoint = coordinator.endpoint().to_string();
            let (failed, fail) = std::sync::mpsc::channel();
            let coordinator_endpoint = endpoint.clone();
            let healthy = std::thread::spawn(move || {
                let mut link = Link::open(Assignment {
                    node_id: 0,
                    coordinator_endpoint,
                });
                Runtime::new().block_on(async move {
                    let registration = RegisterNode {
                        inputs: Vec::new(),
                        outputs: Vec::new(),
                    };
                    link.sender.send(&SERIALIZER.serialize(&registration)).await;
                    // The launcher ends this node once it has seen the other one fail.
                    fail.recv().unwrap();
                });
            });
            let failing = std::thread::spawn(move || {
                // This node takes its code and fails before it describes its ports.
                Link::open(Assignment {
                    node_id: 1,
                    coordinator_endpoint: endpoint,
                });
                failed.send(()).unwrap();
            });
            let two_nodes = Graph {
                nodes: 2,
                connections: Vec::new(),
            };
            assert_eq!(coordinator.run(two_nodes, HashMap::new()).await, Ok(()));
            healthy.join().unwrap();
            failing.join().unwrap();
        });
    }

    #[test]
    fn every_node_gets_its_wiring_once_all_have_registered() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::new(TRANSPORT, SERIALIZER).await;
            let children = spawn_nodes(coordinator.endpoint());
            let wired = coordinator.run(graph(fan_out()), HashMap::new()).await;
            assert_eq!(wired, Ok(()));
            let wirings: Vec<Wiring> = children
                .into_iter()
                .map(|child| child.join().unwrap().1.unwrap())
                .collect();
            assert_eq!(wirings[0][&0].len(), 2);
            assert!(wirings[2].is_empty()); // The logger has no outputs.
        });
    }

    #[test]
    fn a_graph_that_does_not_fit_is_reported_to_every_node() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::new(TRANSPORT, SERIALIZER).await;
            let children = spawn_nodes(coordinator.endpoint());
            let one_connection = graph(fan_out()[..1].to_vec());
            let refused = coordinator.run(one_connection, HashMap::new()).await;
            assert_eq!(
                refused,
                Err("input port 3 of node 2 has no connection".to_string())
            );
            for child in children {
                assert_eq!(
                    child.join().unwrap().1,
                    refused.clone().map(|_| Wiring::default())
                );
            }
        });
    }

    /// Register a node whose current tag is `now`; return the message that shuts it down.
    fn spawn_node_until_shutdown(
        coordinator_endpoint: String,
        node_id: usize,
        now: Time,
        request: Option<Time>,
    ) -> std::thread::JoinHandle<CoordinatorMessage> {
        std::thread::spawn(move || {
            let link = Link::open(Assignment {
                node_id,
                coordinator_endpoint,
            });
            Runtime::new().block_on(async move {
                let (mut sender, mut receiver) = (link.sender, link.receiver);
                let registration = RegisterNode {
                    inputs: Vec::new(),
                    outputs: Vec::new(),
                };
                let reply: Reply = SERIALIZER
                    .deserialize(&sender.call(&SERIALIZER.serialize(&registration)).await);
                reply.unwrap();
                if let Some(delay) = request {
                    let request = NodeMessage::RequestShutdown(delay);
                    sender.send(&SERIALIZER.serialize(&request)).await;
                }
                loop {
                    match SERIALIZER.deserialize(&receiver.recv().await.unwrap()) {
                        CoordinatorMessage::RequestNodeShutdown(delay) => {
                            let offer = NodeMessage::ShutdownTime(now + delay);
                            sender.send(&SERIALIZER.serialize(&offer)).await;
                        }
                        shutdown => return shutdown,
                    }
                }
            })
        })
    }

    #[test]
    fn every_node_shuts_down_at_the_latest_tag_that_any_of_them_offers() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::new(TRANSPORT, SERIALIZER).await;
            let endpoint = coordinator.endpoint().to_string();
            let tags = [Time::new(3), Time::new(7), Time::new(5)];
            let children: Vec<_> = (0..3)
                .map(|node_id| {
                    // The first node asks for a delay of 10ns after the latest tag.
                    let request = (node_id == 0).then_some(Time::new(10));
                    spawn_node_until_shutdown(endpoint.clone(), node_id, tags[node_id], request)
                })
                .collect();
            let unconnected = Graph {
                nodes: 3,
                connections: Vec::new(),
            };
            assert_eq!(coordinator.run(unconnected, HashMap::new()).await, Ok(()));
            for child in children {
                let shutdown = CoordinatorMessage::Shutdown(Time::new(17));
                assert_eq!(child.join().unwrap(), shutdown);
            }
        });
    }

    #[test]
    fn an_interrupt_tells_every_node_to_shut_down_at_once() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::new(TRANSPORT, SERIALIZER).await;
            let endpoint = coordinator.endpoint().to_string();
            // An interrupt that comes before the nodes are wired reaches them once they are.
            coordinator.interrupt().shutdown_now();
            let children: Vec<_> = (0..3)
                .map(|node_id| {
                    spawn_node_until_shutdown(endpoint.clone(), node_id, Time::ZERO, None)
                })
                .collect();
            let unconnected = Graph {
                nodes: 3,
                connections: Vec::new(),
            };
            assert_eq!(coordinator.run(unconnected, HashMap::new()).await, Ok(()));
            for child in children {
                assert_eq!(child.join().unwrap(), CoordinatorMessage::ShutdownNow);
            }
        });
    }

    #[test]
    #[should_panic(expected = "node 0 asked for its code twice")]
    fn duplicate_node_id_is_a_launch_bug() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::new(TRANSPORT, SERIALIZER).await;
            let endpoint = coordinator.endpoint().to_string();
            for _ in 0..2 {
                let registration = RegisterNode {
                    inputs: Vec::new(),
                    outputs: Vec::new(),
                };
                spawn_node(endpoint.clone(), 0, registration);
            }
            let two_nodes = Graph {
                nodes: 2,
                connections: Vec::new(),
            };
            // Both child nodes use the same node ID.
            let _ = coordinator.run(two_nodes, HashMap::new()).await;
        });
    }
}
