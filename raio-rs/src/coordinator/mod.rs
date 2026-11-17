//! The coordinator of an app. Every node connects to it and goes through three stages: it gets its
//! code, registers its ports and gets its wiring, and then runs, sending a heartbeat, until it
//! ends. Every shutdown of an app comes from its coordinator: at a timestamp that the nodes agree
//! on, or at once when the app is interrupted or one of its nodes fails. `Link` is the node's
//! side.

mod graph;
mod link;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::pin::pin;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures::future::{Either, join_all, select};
use futures::{StreamExt, stream};

use crate::driver::Timer;
use crate::messages::{Code, CoordinatorMessage, NodeMessage, RegisterNode, Reply, RequestCode};
use crate::node::NodeId;
use crate::runtime::Runtime;
use crate::serializer::Serializer;
use crate::serializer::bincode::BincodeSerializer;
use crate::time::Time;
use crate::transport::tcp::TcpTransport;
use crate::transport::{Listener, Receiver, Sender, Transport};
pub use graph::{Connection, Graph, PortId};
pub use link::Link;

/// The transport between nodes and their coordinator; both sides use this one.
pub const TRANSPORT: TcpTransport = TcpTransport;
/// The serializer of the messages between nodes and their coordinator.
pub const SERIALIZER: BincodeSerializer = BincodeSerializer;
/// The time between two heartbeats of a running node.
pub const HEARTBEAT_PERIOD: Duration = Duration::from_millis(500);
/// A running node that the coordinator has not heard from for this long has failed.
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);

/// Serves the nodes of one app, from their first connection until the last of them ends.
pub struct Coordinator<T: Transport, S> {
    listener: T::Listener,
    serializer: S,
    handle: CoordinatorHandle,
    reports: UnboundedReceiver<Report>,
    heartbeat_timeout: Duration,
}

/// What a launcher reports to its coordinator.
enum Report {
    /// The user interrupted the app.
    Interrupted,
    /// The process of this node has exited; the flag is true if it succeeded.
    NodeExited(NodeId, bool),
}

/// A launcher's line to its coordinator; any thread may hold one.
#[derive(Clone)]
pub struct CoordinatorHandle(UnboundedSender<Report>);

impl CoordinatorHandle {
    /// Have every node shut down at once.
    pub fn shutdown_now(&self) {
        // A coordinator that has finished has no node left to tell.
        let _ = self.0.unbounded_send(Report::Interrupted);
    }

    /// Report that the process of a node has exited. A node that failed ends the app.
    pub fn node_exited(&self, node_id: NodeId, success: bool) {
        let _ = self.0.unbounded_send(Report::NodeExited(node_id, success));
    }
}

/// The reason a coordinator ended its app early.
#[derive(Clone, PartialEq, Debug)]
pub enum Failure {
    /// The graph does not fit the ports that the nodes registered.
    Refused(String),
    /// Nodes failed, and the coordinator shut the others down.
    Nodes {
        /// The nodes that failed, the first of them first.
        failed: Vec<NodeId>,
        /// Those of them that went silent; their processes may still run.
        silent: Vec<NodeId>,
    },
}

/// What the coordinator of a running app reacts to.
enum Event {
    /// A node sent this message, or closed its link if there is none.
    Node(NodeId, Option<Vec<u8>>),
    Report(Report),
    /// It is time to look for nodes that have gone silent.
    Tick,
}

impl<T, S> Coordinator<T, S>
where
    T: Transport,
    S: Serializer<RequestCode> + Serializer<Code>,
    S: Serializer<RegisterNode> + Serializer<Reply>,
    S: Serializer<NodeMessage> + Serializer<CoordinatorMessage>,
{
    pub async fn new(transport: T, serializer: S) -> Self {
        let (report, reports) = unbounded();
        Self {
            listener: transport.bind().await,
            serializer,
            handle: CoordinatorHandle(report),
            reports,
            heartbeat_timeout: HEARTBEAT_TIMEOUT,
        }
    }

    pub fn endpoint(&self) -> &str {
        self.listener.endpoint()
    }

    /// Return what the app's launcher reports to this coordinator with.
    pub fn handle(&self) -> CoordinatorHandle {
        self.handle.clone()
    }

    /// Give every node its code, wire the nodes once all have registered, then serve them until
    /// every node has ended.
    pub async fn run(
        mut self,
        graph: Graph,
        mut code: HashMap<NodeId, Vec<u8>>,
    ) -> Result<(), Failure> {
        let mut nodes = BTreeMap::new();
        let mut interrupted = false;
        let registered = {
            let (listener, serializer) = (&mut self.listener, &self.serializer);
            let register = async {
                // Every node first asks for its code.
                for _ in 0..graph.nodes {
                    let (mut replier, mut receiver) = listener.accept().await;
                    let request: RequestCode =
                        serializer.deserialize(&receiver.recv().await.unwrap());
                    let node_id = request.node_id;
                    let reply: Code = code.remove(&node_id);
                    replier.send(&serializer.serialize(&reply)).await;
                    assert!(
                        nodes.insert(node_id, (replier, receiver)).is_none(),
                        "node {node_id} asked for its code twice"
                    );
                }
                // Registrations are read once every node has its code.
                let mut registered = BTreeMap::new();
                for (node_id, (_, receiver)) in &mut nodes {
                    let Some(bytes) = receiver.recv().await else {
                        return Err(*node_id);
                    };
                    let registration: RegisterNode = serializer.deserialize(&bytes);
                    registered.insert(*node_id, registration);
                }
                Ok(registered)
            };
            // A node whose process exits before the app is wired never registers.
            let exit = async {
                loop {
                    match self.reports.next().await.unwrap() {
                        Report::NodeExited(node_id, _) => return node_id,
                        // An interrupt before the nodes are wired reaches them once they are.
                        Report::Interrupted => interrupted = true,
                    }
                }
            };
            match select(pin!(register), pin!(exit)).await {
                Either::Left((registered, _)) => registered,
                Either::Right((node_id, _)) => Err(node_id),
            }
        };
        let registered = match registered {
            Ok(registered) => registered,
            Err(node_id) => {
                // The app does not start; every node that has come this far is told so.
                nodes.remove(&node_id);
                let cancelled = self.serializer.serialize(&Reply::Cancelled);
                for (replier, _) in nodes.values_mut() {
                    replier.send(&cancelled).await;
                }
                return Err(Failure::Nodes {
                    failed: vec![node_id],
                    silent: Vec::new(),
                });
            }
        };
        let mut wirings = graph.wire(&registered);
        for (node_id, (replier, _)) in &mut nodes {
            let reply = match &mut wirings {
                Ok(wirings) => Reply::Wired(wirings.remove(node_id).unwrap()),
                Err(reason) => Reply::Refused(reason.clone()),
            };
            replier.send(&self.serializer.serialize(&reply)).await;
        }
        wirings.map_err(Failure::Refused)?;
        self.coordinate_shutdown(nodes, interrupted).await
    }

    /// Serve the running nodes until all of them have ended. A shutdown that a node asks for
    /// happens at the latest timestamp that any node offers; an interrupt or the failure of a node
    /// shuts every node down at once.
    async fn coordinate_shutdown(
        mut self,
        nodes: BTreeMap<NodeId, (T::Sender, T::Receiver)>,
        mut interrupted: bool,
    ) -> Result<(), Failure> {
        let (events, received) = unbounded();
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
        let timeout = self.heartbeat_timeout;
        let ticks = pin!(stream::unfold((), |()| async move {
            Timer::after(timeout / 4).await;
            Some((Event::Tick, ()))
        }));
        let from_nodes = received.map(|(node_id, bytes)| Event::Node(node_id, bytes));
        let reports = (&mut self.reports).map(Event::Report);
        let mut events = stream::select(stream::select(from_nodes, reports), ticks);
        let serializer = &self.serializer;
        let serve = async {
            // The nodes still to offer a timestamp, once a shutdown has been requested.
            let mut waiting: Option<BTreeSet<NodeId>> = None;
            let mut latest = Time::NEVER;
            let (mut failed, mut silent) = (Vec::new(), Vec::new());
            // The last time each running node was heard from.
            let mut heard: BTreeMap<NodeId, Instant> = senders
                .keys()
                .map(|node_id| (*node_id, Instant::now()))
                .collect();
            if interrupted {
                let now = CoordinatorMessage::ShutdownNow;
                Self::broadcast(serializer, &mut senders, now).await;
            }
            while !senders.is_empty() {
                // The nodes that the event shows to have failed.
                let failures: Vec<NodeId> = match events.next().await.unwrap() {
                    Event::Report(Report::Interrupted) => Vec::new(),
                    Event::Report(Report::NodeExited(_, true)) => continue,
                    Event::Report(Report::NodeExited(node_id, false)) => vec![node_id],
                    Event::Tick => {
                        let overdue = heard.iter().filter(|(_, at)| at.elapsed() > timeout);
                        let overdue: Vec<NodeId> = overdue.map(|(node_id, _)| *node_id).collect();
                        if overdue.is_empty() {
                            continue;
                        }
                        silent.extend(&overdue);
                        overdue
                    }
                    Event::Node(node_id, bytes) => {
                        // A node that the coordinator has given up on may still send.
                        if !senders.contains_key(&node_id) {
                            continue;
                        }
                        heard.insert(node_id, Instant::now());
                        match bytes.map(|bytes| serializer.deserialize(&bytes)) {
                            Some(NodeMessage::Heartbeat) => continue,
                            Some(NodeMessage::RequestShutdown(delay)) => {
                                // One shutdown serves every request.
                                if waiting.is_none() && !interrupted {
                                    let ask = CoordinatorMessage::RequestNodeShutdown(delay);
                                    Self::broadcast(serializer, &mut senders, ask).await;
                                    waiting = Some(senders.keys().copied().collect());
                                }
                                continue;
                            }
                            Some(NodeMessage::ShutdownTime(time)) => latest = latest.max(time),
                            None => {
                                senders.remove(&node_id);
                                heard.remove(&node_id);
                            }
                        }
                        let Some(waiting) = &mut waiting else {
                            continue;
                        };
                        if waiting.remove(&node_id) && waiting.is_empty() {
                            let shutdown = CoordinatorMessage::Shutdown(latest);
                            Self::broadcast(serializer, &mut senders, shutdown).await;
                        }
                        continue;
                    }
                };
                // An interrupt or a failure replaces a shutdown that is under way, and the nodes
                // are told once. A node that is only slow still shuts down when it hears this.
                if !interrupted {
                    let now = CoordinatorMessage::ShutdownNow;
                    Self::broadcast(serializer, &mut senders, now).await;
                }
                (interrupted, waiting) = (true, None);
                for node_id in failures {
                    // A node that has failed is not waited for.
                    senders.remove(&node_id);
                    heard.remove(&node_id);
                    if !failed.contains(&node_id) {
                        failed.push(node_id);
                    }
                }
            }
            match failed.is_empty() {
                true => Ok(()),
                false => Err(Failure::Nodes { failed, silent }),
            }
        };
        match select(pin!(serve), join_all(readers)).await {
            Either::Left((outcome, _)) => outcome,
            // Every link has closed, and the events that they left are still to be served.
            Either::Right((_, serve)) => serve.await,
        }
    }

    async fn broadcast(
        serializer: &S,
        senders: &mut BTreeMap<NodeId, T::Sender>,
        message: CoordinatorMessage,
    ) {
        let bytes = serializer.serialize(&message);
        for sender in senders.values_mut() {
            sender.send(&bytes).await;
        }
    }
}

/// Run a coordinator for the graph on its own thread; return its endpoint, the thread, and
/// what the launcher reports to it with.
///
/// `code` holds what each node gets when it asks for its code; a node written in Rust has none.
pub fn spawn(
    graph: Graph,
    code: HashMap<NodeId, Vec<u8>>,
) -> (String, JoinHandle<Result<(), Failure>>, CoordinatorHandle) {
    let (announce, announced) = std::sync::mpsc::channel();
    let coordinator = std::thread::spawn(move || {
        Runtime::new().block_on(async move {
            let coordinator = Coordinator::new(TRANSPORT, SERIALIZER).await;
            let endpoint = coordinator.endpoint().to_string();
            announce.send((endpoint, coordinator.handle())).unwrap();
            coordinator.run(graph, code).await
        })
    });
    let (endpoint, handle) = announced.recv().unwrap();
    (endpoint, coordinator, handle)
}

#[cfg(test)]
mod tests {
    use super::graph::tests::{fan_out, graph, registrations};
    use super::*;
    use crate::messages::Wiring;

    /// Use a separate thread and runtime to simulate each node process.
    fn spawn_node(
        coordinator_endpoint: String,
        node_id: NodeId,
        registration: RegisterNode,
    ) -> std::thread::JoinHandle<(Code, Reply)> {
        std::thread::spawn(move || {
            let mut link = Link::open(node_id, &coordinator_endpoint);
            Runtime::new().block_on(async move {
                let reply = link.register(&registration).await;
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

    fn no_ports() -> RegisterNode {
        RegisterNode {
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }

    fn unconnected(nodes: usize) -> Graph {
        Graph {
            nodes,
            connections: Vec::new(),
        }
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

    /// The failure of nodes that did not go silent.
    fn failed(failed: &[NodeId]) -> Result<(), Failure> {
        Err(Failure::Nodes {
            failed: failed.to_vec(),
            silent: Vec::new(),
        })
    }

    #[test]
    fn a_node_that_fails_before_it_registers_ends_the_app() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::new(TRANSPORT, SERIALIZER).await;
            let endpoint = coordinator.endpoint().to_string();
            let healthy = spawn_node(endpoint.clone(), 0, no_ports());
            // This node takes its code and fails before it describes its ports.
            let failing = std::thread::spawn(move || drop(Link::open(1, &endpoint)));
            let outcome = coordinator.run(unconnected(2), HashMap::new()).await;
            assert_eq!(outcome, failed(&[1]));
            assert_eq!(healthy.join().unwrap().1, Reply::Cancelled);
            failing.join().unwrap();
        });
    }

    #[test]
    fn a_node_whose_process_exits_before_it_connects_ends_the_app() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::new(TRANSPORT, SERIALIZER).await;
            let (endpoint, handle) = (coordinator.endpoint().to_string(), coordinator.handle());
            let healthy = std::thread::spawn(move || {
                let mut link = Link::open(0, &endpoint);
                // The launcher sees the process of node 1 exit; that node never connected.
                handle.node_exited(1, false);
                Runtime::new().block_on(async move { link.register(&no_ports()).await })
            });
            let outcome = coordinator.run(unconnected(2), HashMap::new()).await;
            assert_eq!(outcome, failed(&[1]));
            // The node that connected is not left waiting for its wiring.
            assert_eq!(healthy.join().unwrap(), Reply::Cancelled);
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
                .map(|child| match child.join().unwrap().1 {
                    Reply::Wired(wiring) => wiring,
                    reply => panic!("{reply:?}"),
                })
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
            let reason = "input port 3 of node 2 has no connection".to_string();
            assert_eq!(refused, Err(Failure::Refused(reason.clone())));
            for child in children {
                assert_eq!(child.join().unwrap().1, Reply::Refused(reason.clone()));
            }
        });
    }

    /// How often the nodes of these tests send their heartbeat.
    const BEAT: Duration = Duration::from_millis(5);

    /// Register a node whose current timestamp is `now` and send its heartbeat for as long as
    /// `beats` allows; return the message that shuts the node down.
    fn spawn_node_until_shutdown(
        coordinator_endpoint: String,
        node_id: usize,
        now: Time,
        request: Option<Time>,
        beats: Duration,
    ) -> std::thread::JoinHandle<CoordinatorMessage> {
        std::thread::spawn(move || {
            let link = Link::open(node_id, &coordinator_endpoint);
            Runtime::new().block_on(async move {
                let (mut sender, mut receiver) = (link.sender, link.receiver);
                let reply: Reply =
                    SERIALIZER.deserialize(&sender.call(&SERIALIZER.serialize(&no_ports())).await);
                assert_eq!(reply, Reply::Wired(Wiring::default()));
                if let Some(delay) = request {
                    let request = NodeMessage::RequestShutdown(delay);
                    sender.send(&SERIALIZER.serialize(&request)).await;
                }
                let silent_from = Instant::now() + beats;
                loop {
                    let beat = Timer::after(BEAT);
                    let bytes = match select(pin!(receiver.recv()), beat).await {
                        Either::Left((bytes, _)) => bytes.unwrap(),
                        Either::Right(_) => {
                            if Instant::now() < silent_from {
                                let beat = NodeMessage::Heartbeat;
                                sender.send(&SERIALIZER.serialize(&beat)).await;
                            }
                            continue;
                        }
                    };
                    match SERIALIZER.deserialize(&bytes) {
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

    /// Long enough for a test to end before a node stops sending its heartbeat.
    const ALWAYS: Duration = Duration::from_secs(3600);

    #[test]
    fn every_node_shuts_down_at_the_latest_timestamp_that_any_of_them_offers() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::new(TRANSPORT, SERIALIZER).await;
            let endpoint = coordinator.endpoint().to_string();
            let timestamps = [Time::new(3), Time::new(7), Time::new(5)];
            let children: Vec<_> = (0..3)
                .map(|node_id| {
                    // The first node asks for a delay of 10ns after the latest timestamp.
                    let request = (node_id == 0).then_some(Time::new(10));
                    let now = timestamps[node_id];
                    spawn_node_until_shutdown(endpoint.clone(), node_id, now, request, ALWAYS)
                })
                .collect();
            let outcome = coordinator.run(unconnected(3), HashMap::new()).await;
            assert_eq!(outcome, Ok(()));
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
            coordinator.handle().shutdown_now();
            let children: Vec<_> = (0..3)
                .map(|node_id| {
                    spawn_node_until_shutdown(endpoint.clone(), node_id, Time::ZERO, None, ALWAYS)
                })
                .collect();
            let outcome = coordinator.run(unconnected(3), HashMap::new()).await;
            assert_eq!(outcome, Ok(()));
            for child in children {
                assert_eq!(child.join().unwrap(), CoordinatorMessage::ShutdownNow);
            }
        });
    }

    #[test]
    fn the_failure_of_a_running_node_shuts_the_others_down_at_once() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::new(TRANSPORT, SERIALIZER).await;
            let endpoint = coordinator.endpoint().to_string();
            let children: Vec<_> = (0..2)
                .map(|node_id| {
                    spawn_node_until_shutdown(endpoint.clone(), node_id, Time::ZERO, None, ALWAYS)
                })
                .collect();
            let handle = coordinator.handle();
            let failing = std::thread::spawn(move || {
                let mut link = Link::open(2, &endpoint);
                Runtime::new().block_on(async move { link.register(&no_ports()).await });
                // The node's process fails once the app runs, and its launcher sees that.
                handle.node_exited(2, false);
            });
            let outcome = coordinator.run(unconnected(3), HashMap::new()).await;
            assert_eq!(outcome, failed(&[2]));
            for child in children {
                assert_eq!(child.join().unwrap(), CoordinatorMessage::ShutdownNow);
            }
            failing.join().unwrap();
        });
    }

    #[test]
    fn a_node_that_goes_silent_is_given_up_on_and_the_others_shut_down() {
        Runtime::new().block_on(async {
            let mut coordinator = Coordinator::new(TRANSPORT, SERIALIZER).await;
            coordinator.heartbeat_timeout = 8 * BEAT;
            let endpoint = coordinator.endpoint().to_string();
            let healthy = spawn_node_until_shutdown(endpoint.clone(), 0, Time::ZERO, None, ALWAYS);
            // This node sends its heartbeat for several timeouts and then hangs.
            let heartbeats = 4 * coordinator.heartbeat_timeout;
            let silent = spawn_node_until_shutdown(endpoint, 1, Time::ZERO, None, heartbeats);
            let start = Instant::now();
            let outcome = coordinator.run(unconnected(2), HashMap::new()).await;
            // The coordinator does not wait for the silent node, whose link is still open.
            let gave_up = Failure::Nodes {
                failed: vec![1],
                silent: vec![1],
            };
            assert_eq!(outcome, Err(gave_up));
            assert!(
                start.elapsed() >= heartbeats,
                "a node that sends its heartbeat has not failed"
            );
            assert_eq!(healthy.join().unwrap(), CoordinatorMessage::ShutdownNow);
            // The silent node is told as well, in case it is only slow.
            assert_eq!(silent.join().unwrap(), CoordinatorMessage::ShutdownNow);
        });
    }

    #[test]
    #[should_panic(expected = "node 0 asked for its code twice")]
    fn duplicate_node_id_is_a_launch_bug() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::new(TRANSPORT, SERIALIZER).await;
            let endpoint = coordinator.endpoint().to_string();
            for _ in 0..2 {
                spawn_node(endpoint.clone(), 0, no_ports());
            }
            // Both child nodes use the same node ID.
            let _ = coordinator.run(unconnected(2), HashMap::new()).await;
        });
    }
}
