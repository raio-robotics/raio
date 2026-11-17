use std::collections::{BTreeMap, BTreeSet};
use std::thread::JoinHandle;

use futures::StreamExt;
use futures::channel::mpsc::unbounded;
use futures::future::join_all;

use crate::app::Graph;
use crate::messages::{CoordinatorMessage, NodeMessage, RegisterNode, Reply, Wiring};
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
}

impl<T, S> Coordinator<T, S>
where
    T: Transport,
    S: Serializer<RegisterNode> + Serializer<Reply>,
    S: Serializer<NodeMessage> + Serializer<CoordinatorMessage>,
{
    pub async fn new(transport: T, serializer: S) -> Self {
        Self {
            listener: transport.bind().await,
            serializer,
        }
    }

    pub fn endpoint(&self) -> &str {
        self.listener.endpoint()
    }

    /// Wire the nodes once all have registered, then serve them until every node disconnects.
    pub async fn run(mut self, graph: Graph) -> Result<(), String> {
        let mut nodes = BTreeMap::new();
        let mut registered = BTreeMap::new();
        for _ in 0..graph.nodes {
            let (replier, mut receiver) = self.listener.accept().await;
            let registration: RegisterNode =
                self.serializer.deserialize(&receiver.recv().await.unwrap());
            let node_id = registration.node_id;
            assert!(
                registered.insert(node_id, registration).is_none(),
                "node {node_id} registered twice"
            );
            nodes.insert(node_id, (replier, receiver));
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
    async fn coordinate_shutdown(&self, nodes: BTreeMap<usize, (T::Sender, T::Receiver)>) {
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
            while let Some((node_id, bytes)) = received.next().await {
                match bytes.map(|bytes| self.serializer.deserialize(&bytes)) {
                    Some(NodeMessage::RequestShutdown(delay)) => {
                        // One shutdown serves every request.
                        if waiting.is_none() {
                            let ask = CoordinatorMessage::RequestNodeShutdown(delay);
                            self.broadcast(&mut senders, ask).await;
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
                    self.broadcast(&mut senders, shutdown).await;
                }
            }
        };
        futures::join!(join_all(readers), agree);
    }

    async fn broadcast(
        &self,
        senders: &mut BTreeMap<usize, T::Sender>,
        message: CoordinatorMessage,
    ) {
        let bytes = self.serializer.serialize(&message);
        for sender in senders.values_mut() {
            sender.send(&bytes).await;
        }
    }
}

/// Describe this node's ports to the coordinator; return their wiring and the open connection.
pub async fn register<T: Transport, S: Serializer<RegisterNode> + Serializer<Reply>>(
    transport: &T,
    serializer: &S,
    coordinator_endpoint: &str,
    registration: RegisterNode,
) -> Result<(Wiring, T::Sender, T::Receiver), String> {
    let (mut sender, receiver) = transport.connect(coordinator_endpoint).await;
    let request = serializer.serialize(&registration);
    let reply: Reply = serializer.deserialize(&sender.call(&request).await);
    reply.map(|wiring| (wiring, sender, receiver))
}

/// Run a coordinator for the graph on its own thread; return its endpoint and the thread.
pub fn spawn(graph: Graph) -> (String, JoinHandle<Result<(), String>>) {
    let (announce, announced) = std::sync::mpsc::channel();
    let coordinator = std::thread::spawn(move || {
        Runtime::new().block_on(async move {
            let coordinator = Coordinator::new(TRANSPORT, SERIALIZER).await;
            announce.send(coordinator.endpoint().to_string()).unwrap();
            coordinator.run(graph).await
        })
    });
    (announced.recv().unwrap(), coordinator)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::graph::tests::{fan_out, graph, registrations};
    use crate::messages::Wiring;

    /// Use a separate thread and runtime to simulate each node process.
    fn spawn_node(
        coordinator: String,
        registration: RegisterNode,
    ) -> std::thread::JoinHandle<Reply> {
        std::thread::spawn(move || {
            Runtime::new().block_on(async move {
                let registered = register(&TRANSPORT, &SERIALIZER, &coordinator, registration);
                // Dropping the connection tells the coordinator that the node finished.
                registered.await.map(|(wiring, _, _)| wiring)
            })
        })
    }

    #[test]
    fn every_node_gets_its_wiring_once_all_have_registered() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::new(TRANSPORT, SERIALIZER).await;
            let endpoint = coordinator.endpoint().to_string();
            let children: Vec<_> = registrations()
                .into_iter()
                .map(|registration| spawn_node(endpoint.clone(), registration))
                .collect();
            assert_eq!(coordinator.run(graph(fan_out())).await, Ok(()));
            let wirings: Vec<Wiring> = children
                .into_iter()
                .map(|child| child.join().unwrap().unwrap())
                .collect();
            assert_eq!(wirings[0][&0].len(), 2);
            assert!(wirings[2].is_empty()); // The logger has no outputs.
        });
    }

    #[test]
    fn a_graph_that_does_not_fit_is_reported_to_every_node() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::new(TRANSPORT, SERIALIZER).await;
            let endpoint = coordinator.endpoint().to_string();
            let children: Vec<_> = registrations()
                .into_iter()
                .map(|registration| spawn_node(endpoint.clone(), registration))
                .collect();
            let refused = coordinator.run(graph(fan_out()[..1].to_vec())).await;
            assert_eq!(
                refused,
                Err("input port 3 of node 2 has no connection".to_string())
            );
            for child in children {
                assert_eq!(
                    child.join().unwrap(),
                    refused.clone().map(|_| Wiring::default())
                );
            }
        });
    }

    /// Register a node whose current tag is `now`; return the shutdown tag the coordinator names.
    fn spawn_node_until_shutdown(
        coordinator: String,
        node_id: usize,
        now: Time,
    ) -> std::thread::JoinHandle<Time> {
        std::thread::spawn(move || {
            Runtime::new().block_on(async move {
                let registration = RegisterNode {
                    node_id,
                    inputs: Vec::new(),
                    outputs: Vec::new(),
                };
                let registered = register(&TRANSPORT, &SERIALIZER, &coordinator, registration);
                let (_, mut sender, mut receiver) = registered.await.unwrap();
                if node_id == 0 {
                    let request = NodeMessage::RequestShutdown(Time::new(10));
                    sender.send(&SERIALIZER.serialize(&request)).await;
                }
                loop {
                    match SERIALIZER.deserialize(&receiver.recv().await.unwrap()) {
                        CoordinatorMessage::RequestNodeShutdown(delay) => {
                            let offer = NodeMessage::ShutdownTime(now + delay);
                            sender.send(&SERIALIZER.serialize(&offer)).await;
                        }
                        CoordinatorMessage::Shutdown(tag) => return tag,
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
                .map(|node_id| spawn_node_until_shutdown(endpoint.clone(), node_id, tags[node_id]))
                .collect();
            let unconnected = Graph {
                nodes: 3,
                connections: Vec::new(),
            };
            assert_eq!(coordinator.run(unconnected).await, Ok(()));
            for child in children {
                // The first node asked for a delay of 10ns after the latest tag.
                assert_eq!(child.join().unwrap(), Time::new(17));
            }
        });
    }

    #[test]
    #[should_panic(expected = "node 0 registered twice")]
    fn duplicate_node_id_is_a_launch_bug() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::new(TRANSPORT, SERIALIZER).await;
            let endpoint = coordinator.endpoint().to_string();
            for _ in 0..2 {
                let registration = RegisterNode {
                    node_id: 0,
                    inputs: Vec::new(),
                    outputs: Vec::new(),
                };
                spawn_node(endpoint.clone(), registration);
            }
            let two_nodes = Graph {
                nodes: 2,
                connections: Vec::new(),
            };
            let _ = coordinator.run(two_nodes).await; // Both child nodes register the same node ID.
        });
    }
}
