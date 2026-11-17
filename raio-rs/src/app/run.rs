use std::cell::RefCell;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::rc::Rc;

use super::builder::Builder;
use super::connection::connect_node;
use crate::coordinator::Link;
use crate::node::{NodeId, NodeScheduler};
use crate::runtime::Runtime;

type Run = Box<dyn FnOnce(Link)>;

/// Wires and runs one built node; taken by the one caller that runs it.
#[derive(Clone)]
pub(super) struct Runner(Rc<RefCell<Option<Run>>>);

impl Runner {
    pub(super) fn run(&self, link: Link) {
        let run = self.0.take().expect("the node has already run");
        run(link);
    }
}

pub type Start<N> = Box<dyn FnOnce(Rc<N>) -> Pin<Box<dyn Future<Output = ()>>>>;

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
    scheduler: Rc<NodeScheduler>,
    pub(super) runner: Runner,
}

impl<N> Deref for CreatedNode<N> {
    type Target = N;

    fn deref(&self) -> &N {
        &self.node
    }
}

impl<N: 'static> CreatedNode<N> {
    pub(super) fn new(
        node_id: NodeId,
        constructor: impl FnOnce(&mut Builder) -> N,
        starts: Vec<Start<N>>,
    ) -> Self {
        let runtime = Rc::new(Runtime::new());
        let scheduler = Rc::new(NodeScheduler::new(runtime.clone()));
        let mut builder = Builder::new(node_id, scheduler.clone());
        let node = Rc::new(constructor(&mut builder));
        let (running, handle) = (node.clone(), scheduler.clone());
        // Wire the node through the coordinator and run it until it finishes.
        let run = move |link: Link| {
            for task in builder.tasks {
                runtime.spawn_internal(task);
            }
            let frame_transfer = runtime.block_on(connect_node(
                scheduler.clone(),
                link,
                builder.inputs,
                builder.outputs,
            ));
            let tasks = starts
                .into_iter()
                .map(|start| start(running.clone()))
                .collect();
            scheduler.run(frame_transfer, tasks);
        };
        Self {
            node,
            scheduler: handle,
            runner: Runner(Rc::new(RefCell::new(Some(Box::new(run))))),
        }
    }

    /// Run this function once when the node shuts down: at the tag at which the app shuts
    /// down, where nothing else runs, or at the node's last tag when it finishes on its own.
    pub fn on_shutdown(self, shutdown: impl FnOnce(&N) + 'static) -> Self {
        let node = self.node.clone();
        *self.scheduler.shutdown_function.borrow_mut() = Some(Box::new(move || shutdown(&node)));
        self
    }
}

/// Build the node that this program runs; set its ports before `run_node`.
pub fn create_node<N: 'static>(
    constructor: impl FnOnce(&mut Builder) -> N,
    starts: Vec<Start<N>>,
) -> CreatedNode<N> {
    // The node's id comes with its assignment; its ports do not use it.
    CreatedNode::new(0, constructor, starts)
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
    run_node_as(assignment, node);
}

/// Run the node under a given assignment, for nodes that share a process.
pub fn run_node_as<N: 'static>(assignment: Assignment, node: CreatedNode<N>) {
    run_node_on(Link::open(assignment), node);
}

/// Run the node on a link that is already open, for a node built from the code the link brought.
pub fn run_node_on<N: 'static>(link: Link, node: CreatedNode<N>) {
    node.runner.run(link);
}

/// Package async node methods as startup tasks for `create_node`.
#[macro_export]
macro_rules! tasks {
    ($($start:expr),+ $(,)?) => {
        vec![$({
            let start: $crate::app::Start<_> =
                Box::new(move |node| Box::pin($start(node)));
            start
        }),+]
    };
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::app::{Connection, Graph, InputPort, Node, OutputPort, PortId};
    use crate::coordinator::spawn;
    use crate::serializer::tests::Decimal;
    use crate::time::{MS, Time};
    use crate::transport::tests::Loopback;

    #[test]
    fn a_connection_uses_the_serializer_and_transport_its_ports_were_set_to() {
        let port = |node_id| PortId {
            node_id,
            port_id: 0,
        };
        let graph = Graph {
            nodes: 2,
            connections: vec![Connection {
                from: port(0),
                to: port(1),
                delay: Time::ZERO,
            }],
        };
        let (coordinator_endpoint, coordinator) = spawn(graph, HashMap::new());
        let assign = |node_id| Assignment {
            node_id,
            coordinator_endpoint: coordinator_endpoint.clone(),
        };

        let assignment = assign(0);
        let source = std::thread::spawn(move || {
            let start: Start<(Node, OutputPort<i64>)> = Box::new(|source| {
                Box::pin(async move {
                    source.0.wait(1 * MS).await;
                    source.1.set(7);
                })
            });
            let source = create_node(|builder| (builder.node(), builder.output()), vec![start]);
            source.1.set_serializer(Decimal);
            source.1.set_transport(Loopback);
            run_node_as(assignment, source);
        });
        let assignment = assign(1);
        let sink = std::thread::spawn(move || {
            let received = Rc::new(std::cell::Cell::new(None));
            let seen = received.clone();
            let start: Start<InputPort<i64>> = Box::new(move |input| {
                Box::pin(async move { seen.set(input.wait().await.map(|(value, _)| *value)) })
            });
            let sink = create_node(|builder| builder.input(), vec![start]);
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
    fn every_node_shuts_down_at_one_tag_and_gets_what_was_sent_before_it() {
        let port = |node_id| PortId {
            node_id,
            port_id: 0,
        };
        let graph = Graph {
            nodes: 2,
            connections: vec![Connection {
                from: port(0),
                to: port(1),
                delay: Time::ZERO,
            }],
        };
        let (coordinator_endpoint, coordinator) = spawn(graph, HashMap::new());
        let assign = |node_id| Assignment {
            node_id,
            coordinator_endpoint: coordinator_endpoint.clone(),
        };

        let assignment = assign(0);
        let source = std::thread::spawn(move || {
            let start: Start<(Node, OutputPort<i64>)> = Box::new(|source| {
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
            let source = create_node(|builder| (builder.node(), builder.output()), vec![start])
                .on_shutdown(move |source| at.set(Some(source.0.now())));
            run_node_as(assignment, source);
            stopped.get()
        });
        let assignment = assign(1);
        let sink = std::thread::spawn(move || {
            let received = Rc::new(RefCell::new(Vec::new()));
            let seen = received.clone();
            let start: Start<(Node, InputPort<i64>)> = Box::new(move |sink| {
                Box::pin(async move {
                    while let Some((value, tag)) = sink.1.wait().await {
                        seen.borrow_mut().push(tag);
                        if *value == 2 {
                            sink.0.request_shutdown_after(2 * MS);
                        }
                    }
                })
            });
            let stopped = Rc::new(std::cell::Cell::new(None));
            let at = stopped.clone();
            let sink = create_node(|builder| (builder.node(), builder.input()), vec![start])
                .on_shutdown(move |sink| at.set(Some(sink.0.now())));
            run_node_as(assignment, sink);
            (received.take(), stopped.get())
        });
        assert_eq!(coordinator.join().unwrap(), Ok(()));
        let shutdown = source.join().unwrap().expect("the source shut down");
        let (received, sink_shutdown) = sink.join().unwrap();
        assert_eq!(sink_shutdown, Some(shutdown));
        // The sink got every tick before the shutdown tag, and none at or after it.
        let ticks: Vec<Time> = (1..)
            .map(|count| count * MS)
            .take_while(|tick| *tick < shutdown)
            .collect();
        assert_eq!(received, ticks);
        // The sink asked at 3ms for a shutdown 2ms after the latest node's tag.
        assert!(shutdown >= 5 * MS, "{shutdown}");
    }

    #[test]
    #[should_panic(expected = "a launcher starts it with --node_id and --coordinator_endpoint")]
    fn a_node_program_started_by_hand_says_what_it_needs() {
        let node: CreatedNode<OutputPort<i64>> =
            create_node(|builder| builder.output(), Vec::new());
        run_node(node);
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
