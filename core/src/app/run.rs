//! Creating one node, and running it in the process that a launcher assigned to it.

use std::cell::RefCell;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::rc::Rc;

use super::builder::{Builder, Ports};
use super::connection::connect_node;
use crate::coordinator::Link;
use crate::diagram::{DiagramNode, DiagramTask};
use crate::node::{NodeId, NodeScheduler};
use crate::runtime::{BoxFuture, Runtime};

type Run = Box<dyn FnOnce(Link)>;

/// One built node as its app holds it: what wires and runs the node, which the one caller that
/// runs it takes, and what a diagram shows of the node.
#[derive(Clone)]
pub(super) struct Runner {
    run: Rc<RefCell<Option<Run>>>,
    pub(super) diagram: DiagramNode,
}

impl Runner {
    pub(super) fn run(&self, link: Link) {
        let run = self.run.take().expect("the node has already run");
        run(link);
    }
}

/// One task or reaction of a node: its name, the triggers of a reaction, and what starts it,
/// which takes the node and returns the future that runs it.
pub struct Start<N> {
    name: &'static str,
    /// The fields that hold the triggers of a reaction; `None` for a task.
    triggers: Option<&'static [&'static str]>,
    start: Box<dyn FnOnce(Rc<N>) -> BoxFuture<()>>,
}

impl<N> Start<N> {
    pub fn task(
        name: &'static str,
        start: impl FnOnce(Rc<N>) -> Pin<Box<dyn Future<Output = ()>>> + 'static,
    ) -> Self {
        Self {
            name,
            triggers: None,
            start: Box::new(start),
        }
    }

    /// A reaction; its future waits for its triggers and runs the reaction's body.
    pub fn reaction(
        name: &'static str,
        triggers: &'static [&'static str],
        start: impl FnOnce(Rc<N>) -> Pin<Box<dyn Future<Output = ()>>> + 'static,
    ) -> Self {
        Self {
            name,
            triggers: Some(triggers),
            start: Box::new(start),
        }
    }
}

/// What a node declares in its `#[node]` block, which implements this trait: its tasks and
/// reactions, and its shutdown function.
pub trait Tasks: Sized {
    /// The tasks and reactions of the node, in the order in which it declares them.
    fn starts(&self) -> Vec<Start<Self>> {
        Vec::new()
    }

    /// Run once when the node shuts down: at the timestamp at which the app shuts down, where
    /// nothing else runs, or at the node's last timestamp when it finishes on its own.
    fn on_shutdown(&self) {}
}

/// A port that is a node by itself has nothing to run.
impl<T> Tasks for super::InputPort<T> {}
impl<T> Tasks for super::OutputPort<T> {}
impl<T> Tasks for super::Action<T> {}

/// A node's place in an app, as its launcher assigned it.
pub struct Assignment {
    pub node_id: NodeId,
    pub coordinator_endpoint: String,
}

impl Assignment {
    const NODE_ID: &str = "--node_id";
    const COORDINATOR_ENDPOINT: &str = "--coordinator_endpoint";

    /// Read the assignment that a launcher put in the program's arguments.
    pub fn from_args() -> Option<Self> {
        Self::parse(std::env::args())
    }

    /// Return `None` without `--node_id`; a launcher that passes it must pass both flags in full.
    fn parse(args: impl IntoIterator<Item = String>) -> Option<Self> {
        let args: Vec<String> = args.into_iter().collect();
        // The launcher's flags come last, after the program's own arguments.
        let value = |flag: &str| {
            let index = args.iter().rposition(|arg| arg == flag)?;
            let value = args.get(index + 1);
            Some(
                value
                    .unwrap_or_else(|| panic!("{flag} needs a value"))
                    .clone(),
            )
        };
        let node_id = value(Self::NODE_ID)?;
        let coordinator_endpoint = value(Self::COORDINATOR_ENDPOINT);
        Some(Self {
            node_id: node_id
                .parse()
                .unwrap_or_else(|_| panic!("{} is not a node id: {node_id:?}", Self::NODE_ID)),
            coordinator_endpoint: coordinator_endpoint.unwrap_or_else(|| {
                panic!("{} needs {}", Self::NODE_ID, Self::COORDINATOR_ENDPOINT)
            }),
        })
    }

    /// Return the program arguments that carry this assignment.
    pub fn args(&self) -> [String; 4] {
        [
            Self::NODE_ID.to_string(),
            self.node_id.to_string(),
            Self::COORDINATOR_ENDPOINT.to_string(),
            self.coordinator_endpoint.clone(),
        ]
    }
}

/// A node that is built and not yet running.
pub struct CreatedNode<N> {
    node: Rc<N>,
    pub(super) runner: Runner,
}

impl<N> Deref for CreatedNode<N> {
    type Target = N;

    fn deref(&self) -> &N {
        &self.node
    }
}

/// Drop the module paths from the name of a type: `a::Camera<b::Image>` is `Camera<Image>`.
fn short(type_name: &str) -> String {
    let mut pieces: Vec<&str> = type_name.split("::").collect();
    let last = pieces.pop().unwrap();
    let is_name = |c: char| c.is_alphanumeric() || c == '_';
    let kept = pieces.iter().map(|piece| piece.trim_end_matches(is_name));
    kept.chain([last]).collect()
}

impl<N: 'static> CreatedNode<N> {
    /// Build a node with the tasks, reactions and shutdown function that it declares.
    pub(super) fn new(node_id: NodeId, constructor: impl FnOnce(&mut Builder) -> N) -> Self
    where
        N: Ports + Tasks,
    {
        Self::with_starts(node_id, constructor, N::starts, N::on_shutdown)
    }

    /// Build a node with given tasks and shutdown function. `starts` lists the tasks and
    /// reactions of the built node.
    fn with_starts(
        node_id: NodeId,
        constructor: impl FnOnce(&mut Builder) -> N,
        starts: impl FnOnce(&N) -> Vec<Start<N>>,
        shutdown: impl FnOnce(&N) + 'static,
    ) -> Self
    where
        N: Ports,
    {
        let runtime = Rc::new(Runtime::new());
        let scheduler = Rc::new(NodeScheduler::new(runtime.clone()));
        let mut builder = Builder::new(node_id, scheduler.clone());
        let node = Rc::new(constructor(&mut builder));
        let tasks = starts(&node);
        let names = node.names();
        let mut ports = builder.ports.clone();
        for (position, name) in &names {
            ports[*position].0 = name.to_string();
        }
        // A trigger is named by the field that holds it; a diagram takes its position.
        let position = |field: &str| {
            let port = names.iter().find(|(_, name)| *name == field);
            port.unwrap_or_else(|| panic!("the trigger `{field}` is not a port of the node"))
                .0
        };
        let shown = tasks.iter().map(|task| DiagramTask {
            name: task.name.to_string(),
            triggers: task
                .triggers
                .map(|fields| fields.iter().map(|field| position(field)).collect()),
        });
        let diagram = DiagramNode {
            name: short(std::any::type_name::<N>()),
            ports,
            tasks: shown.collect(),
        };
        let stopping = node.clone();
        *scheduler.shutdown_function.borrow_mut() = Some(Box::new(move || shutdown(&stopping)));
        let running = node.clone();
        // Wire the node through the coordinator and run it until it finishes.
        let run = move |link: Link| {
            for task in builder.internal_tasks {
                runtime.spawn_internal(task);
            }
            let connected = connect_node(scheduler.clone(), link, builder.inputs, builder.outputs);
            // A node of an app that does not start has nothing to run.
            let Some(frame_transfer) = runtime.block_on(connected) else {
                return;
            };
            let tasks = tasks
                .into_iter()
                .map(|task| (task.start)(running.clone()))
                .collect();
            scheduler.run(frame_transfer, tasks);
        };
        Self {
            node,
            runner: Runner {
                run: Rc::new(RefCell::new(Some(Box::new(run)))),
                diagram,
            },
        }
    }
}

/// Build the node that this program runs; set its ports before `run_node`.
pub fn create_node<N: Ports + Tasks + 'static>(
    constructor: impl FnOnce(&mut Builder) -> N,
) -> CreatedNode<N> {
    // The node's id comes with its assignment; its ports do not use it.
    CreatedNode::new(0, constructor)
}

/// Run the node as this program's launcher assigned it, until it finishes.
#[track_caller]
pub fn run_node<N: 'static>(node: CreatedNode<N>) {
    let Some(assignment) = Assignment::from_args() else {
        panic!(
            "this program is one node of a raio app; a launcher starts it with {} and {}",
            Assignment::NODE_ID,
            Assignment::COORDINATOR_ENDPOINT
        );
    };
    super::interrupt::ignore_interrupt();
    run_node_as(assignment, node);
}

/// Run the node under a given assignment, for nodes that share a process.
pub fn run_node_as<N: 'static>(assignment: Assignment, node: CreatedNode<N>) {
    let link = Link::open(assignment.node_id, &assignment.coordinator_endpoint);
    run_node_on(link, node);
}

/// Run the node on a link that is already open, for a node built from the code the link brought.
pub fn run_node_on<N: 'static>(link: Link, node: CreatedNode<N>) {
    node.runner.run(link);
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::app::{InputPort, Node, OutputPort};
    use crate::coordinator::{Connection, Failure, Graph, PortId, spawn};
    use crate::coordinator::{SERIALIZER, TRANSPORT};
    use crate::messages::{Code, NodeMessage, Reply, Wiring};
    use crate::serializer::Serializer;
    use crate::serializer::tests::Decimal;
    use crate::time::{HOUR, MS, Time};
    use crate::transport::tests::Loopback;
    use crate::transport::{Listener, Receiver, Sender, Transport};

    /// The nodes of these tests have no fields to name their ports after.
    impl<A, B> Ports for (A, B) {}
    impl Ports for Node {}

    /// Build a node from tasks and a shutdown function that the test gives it.
    fn node_with<N: Ports + 'static>(
        constructor: impl FnOnce(&mut Builder) -> N,
        starts: Vec<Start<N>>,
        shutdown: impl FnOnce(&N) + 'static,
    ) -> CreatedNode<N> {
        CreatedNode::with_starts(0, constructor, |_| starts, shutdown)
    }

    /// Start a coordinator for a source, node 1, that feeds a sink, node 2; return what assigns
    /// each node its place, and the coordinator's thread.
    fn pair() -> (
        impl Fn(NodeId) -> Assignment,
        std::thread::JoinHandle<Result<(), Failure>>,
    ) {
        let port = |node_id| PortId {
            node_id,
            port_id: 0,
        };
        let graph = Graph {
            nodes: 2,
            connections: vec![Connection {
                from: port(1),
                to: port(2),
                delay: Time::ZERO,
                physical: false,
            }],
        };
        let (coordinator_endpoint, coordinator, _) = spawn(graph, HashMap::new());
        let assign = move |node_id| Assignment {
            node_id,
            coordinator_endpoint: coordinator_endpoint.clone(),
        };
        (assign, coordinator)
    }

    #[test]
    fn a_connection_uses_the_serializer_and_transport_its_ports_were_set_to() {
        let (assign, coordinator) = pair();

        let assignment = assign(1);
        let source = std::thread::spawn(move || {
            let start = Start::<(Node, OutputPort<i64>)>::task("start", |source| {
                Box::pin(async move {
                    source.0.wait(1 * MS).await;
                    source.1.set(7);
                })
            });
            let source = node_with(
                |builder| (builder.node(), builder.output()),
                vec![start],
                |_| {},
            );
            source.1.set_serializer(Decimal);
            source.1.set_transport(Loopback);
            run_node_as(assignment, source);
        });
        let assignment = assign(2);
        let sink = std::thread::spawn(move || {
            let received = Rc::new(std::cell::Cell::new(None));
            let seen = received.clone();
            let start = Start::<InputPort<i64>>::task("start", move |input| {
                Box::pin(async move { seen.set(input.wait().await.map(|(value, _)| *value)) })
            });
            let sink = node_with(|builder| builder.input(), vec![start], |_| {});
            sink.set_serializer(Decimal);
            sink.set_transport(Loopback);
            run_node_as(assignment, sink);
            received.get()
        });
        assert_eq!(coordinator.join().unwrap(), Ok(()));
        source.join().unwrap();
        assert_eq!(sink.join().unwrap(), Some(7));
    }

    #[test]
    fn every_node_shuts_down_at_one_timestamp_and_gets_what_was_sent_before_it() {
        let (assign, coordinator) = pair();

        let assignment = assign(1);
        let source = std::thread::spawn(move || {
            let start = Start::<(Node, OutputPort<i64>)>::task("start", |source| {
                Box::pin(async move {
                    // This task never ends on its own.
                    let mut count = 0;
                    loop {
                        source.0.wait(1 * MS).await;
                        source.1.set(count);
                        count += 1;
                    }
                })
            });
            let stopped = Rc::new(std::cell::Cell::new(None));
            let at = stopped.clone();
            let source = node_with(
                |builder| (builder.node(), builder.output()),
                vec![start],
                move |source| at.set(Some(source.0.now())),
            );
            run_node_as(assignment, source);
            stopped.get()
        });
        let assignment = assign(2);
        let sink = std::thread::spawn(move || {
            let received = Rc::new(RefCell::new(Vec::new()));
            let seen = received.clone();
            let start = Start::<(Node, InputPort<i64>)>::task("start", move |sink| {
                Box::pin(async move {
                    while let Some((value, timestamp)) = sink.1.wait().await {
                        seen.borrow_mut().push(timestamp);
                        if *value == 2 {
                            sink.0.request_shutdown_after(2 * MS);
                        }
                    }
                })
            });
            let stopped = Rc::new(std::cell::Cell::new(None));
            let at = stopped.clone();
            let sink = node_with(
                |builder| (builder.node(), builder.input()),
                vec![start],
                move |sink| at.set(Some(sink.0.now())),
            );
            run_node_as(assignment, sink);
            (received.take(), stopped.get())
        });
        assert_eq!(coordinator.join().unwrap(), Ok(()));
        let shutdown = source.join().unwrap().expect("the source shut down");
        let (received, sink_shutdown) = sink.join().unwrap();
        assert_eq!(sink_shutdown, Some(shutdown));
        // The sink got every tick before the shutdown timestamp, and none at or after it.
        let ticks: Vec<Time> = (1..)
            .map(|count| count * MS)
            .take_while(|tick| *tick < shutdown)
            .collect();
        assert_eq!(received, ticks);
        // The sink asked at 3ms for a shutdown 2ms after the latest node's timestamp.
        assert!(shutdown >= 5 * MS, "{shutdown}");
    }

    /// Start a coordinator of one node that answers the node's registration with `reply`,
    /// returns the node's next message if it wired the node, and goes away.
    fn lone_coordinator(
        reply: Reply,
    ) -> (Assignment, std::thread::JoinHandle<Option<NodeMessage>>) {
        let (announce, announced) = std::sync::mpsc::channel();
        let coordinator = std::thread::spawn(move || {
            Runtime::new().block_on(async move {
                let mut listener = TRANSPORT.bind().await;
                announce.send(listener.endpoint().to_string()).unwrap();
                let (mut replier, mut receiver) = listener.accept().await;
                receiver.recv().await.unwrap();
                let code: Code = None;
                replier.send(&SERIALIZER.serialize(&code)).await;
                receiver.recv().await.unwrap();
                replier.send(&SERIALIZER.serialize(&reply)).await;
                match reply {
                    Reply::Wired(_) => receiver.recv().await,
                    _ => None,
                }
                .map(|bytes| SERIALIZER.deserialize(&bytes))
            })
        });
        let assignment = Assignment {
            node_id: 1,
            coordinator_endpoint: announced.recv().unwrap(),
        };
        (assignment, coordinator)
    }

    /// Create a node whose task outlasts the test, and the flags that its task and its
    /// shutdown function set.
    fn waiting_node() -> (CreatedNode<Node>, [Rc<std::cell::Cell<bool>>; 2]) {
        let flags = [(); 2].map(|_| Rc::new(std::cell::Cell::new(false)));
        let [started, shut_down] = flags.clone();
        let start = Start::<Node>::task("start", move |node| {
            Box::pin(async move {
                started.set(true);
                node.wait(HOUR).await;
            })
        });
        let node = node_with(
            |builder| builder.node(),
            vec![start],
            move |_| shut_down.set(true),
        );
        (node, flags)
    }

    #[test]
    fn a_node_sends_its_heartbeat_and_shuts_down_once_its_coordinator_is_gone() {
        let (assignment, coordinator) = lone_coordinator(Reply::Wired(Wiring::default()));
        let (node, [started, shut_down]) = waiting_node();
        run_node_as(assignment, node);
        assert_eq!(coordinator.join().unwrap(), Some(NodeMessage::Heartbeat));
        assert!(started.get() && shut_down.get());
    }

    #[test]
    fn a_node_of_an_app_that_does_not_start_runs_nothing() {
        let (assignment, coordinator) = lone_coordinator(Reply::Cancelled);
        let (node, [started, shut_down]) = waiting_node();
        run_node_as(assignment, node);
        coordinator.join().unwrap();
        assert!(!started.get() && !shut_down.get());
    }

    #[test]
    #[should_panic(expected = "a launcher starts it with --node_id and --coordinator_endpoint")]
    fn a_node_program_started_by_hand_says_what_it_needs() {
        let node: CreatedNode<OutputPort<i64>> = create_node(|builder| builder.output());
        run_node(node);
    }

    #[test]
    fn a_type_is_shown_without_the_paths_of_its_modules() {
        assert_eq!(short("hello::Producer"), "Producer");
        let pair = "(raio::node::Node, raio::app::port::OutputPort<i64>)";
        assert_eq!(short(pair), "(Node, OutputPort<i64>)");
    }

    fn program(args: &[&str]) -> Vec<String> {
        args.iter().map(|arg| arg.to_string()).collect()
    }

    #[test]
    fn an_assignment_travels_through_the_program_arguments() {
        let assignment = Assignment {
            node_id: 3,
            coordinator_endpoint: "127.0.0.1:9".to_string(),
        };
        let flags = assignment.args();
        assert_eq!(
            flags,
            ["--node_id", "3", "--coordinator_endpoint", "127.0.0.1:9"]
        );
        // The flags follow the program's own arguments.
        let mut args = program(&["node", "hello world"]);
        args.extend(flags);
        let parsed = Assignment::parse(args).unwrap();
        assert_eq!(parsed.node_id, 3);
        assert_eq!(parsed.coordinator_endpoint, "127.0.0.1:9");
    }

    #[test]
    fn a_program_started_by_hand_has_no_assignment() {
        assert!(Assignment::parse(program(&["node", "hello world"])).is_none());
    }

    #[test]
    #[should_panic(expected = "--node_id is not a node id: \"first\"")]
    fn a_malformed_assignment_is_a_launch_bug() {
        Assignment::parse(program(&["node", "--node_id", "first"]));
    }
}
