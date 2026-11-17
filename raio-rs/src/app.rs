pub use crate::action::{Action, ActionHandle};
use crate::coordinator::{Coordinator, register};
use crate::graph::{Connection, Graph, PortRef};
use crate::messages::{Downstream, Frame, PortInfo, Upstream};
pub use crate::node::{ExecutionMode, Node};
use crate::node::{NodeId, NodeScheduler};
pub use crate::port::{Builder, InputPort, OutputPort, PortId};
use crate::port::{InputChannel, OutputChannel};
use crate::runtime::{BoxFuture, Runtime};
use crate::serializer::Serializer;
use crate::serializer::bincode::BincodeSerializer;
use crate::time::Time;
use crate::transport::tcp::TcpTransport;
use futures::channel::mpsc::unbounded;
use futures::future::join_all;
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::rc::Rc;

/// Wires and runs one built node; taken by the one caller that runs it.
type Runner = Rc<RefCell<Option<Box<dyn FnOnce(Assignment)>>>>;
pub type Start<N> = Box<dyn FnOnce(Rc<N>) -> Pin<Box<dyn Future<Output = ()>>>>;

/// Builds an input's frame-receiving task once its upstream is known.
pub(crate) type Receive = Box<dyn FnOnce(Upstream) -> BoxFuture<()>>;

/// An input's listening address for each of its transports.
pub(crate) type Endpoints = HashMap<String, String>;

/// An input port whose payload type is hidden from the node's wiring.
pub(crate) trait InputEnd {
    /// Describe the port for registration, without its addresses.
    fn describe(&self) -> PortInfo;
    /// Listen with each of the port's transports; return the addresses and receiving task builder.
    fn listen(self: Rc<Self>, scheduler: Rc<NodeScheduler>) -> BoxFuture<(Endpoints, Receive)>;
}

/// An output port whose payload type is hidden from the node's wiring.
pub(crate) trait OutputEnd {
    fn describe(&self) -> PortInfo;
    /// Attach a sender for one downstream input and return the task that sends its frames.
    fn connect_to(self: Rc<Self>, downstream: Downstream) -> BoxFuture<()>;
}

/// Apply the edge delay, queue the frame's value, and update the input's ENT.
fn deliver<T>(
    serializer: &dyn Serializer<T>,
    delay: Time,
    channel: &InputChannel<T>,
    scheduler: &NodeScheduler,
    message: Frame,
) {
    if let Some(bytes) = message.value {
        let value = serializer.deserialize(&bytes);
        if message.physical {
            // Move late physical events to a tag after the node's current tag.
            let tag = (message.timestamp + delay).max(scheduler.now().next_microstep());
            channel.push_physical(tag, value);
        } else {
            channel.push(message.timestamp + delay, value);
        }
    }
    channel.update_remote_ent(message.ent + delay);
}

impl<T: 'static> InputEnd for InputChannel<T> {
    fn describe(&self) -> PortInfo {
        self.spec.describe()
    }

    fn listen(self: Rc<Self>, scheduler: Rc<NodeScheduler>) -> BoxFuture<(Endpoints, Receive)> {
        Box::pin(async move {
            let mut endpoints = HashMap::new();
            let mut accepts = HashMap::new();
            for transport in self.spec.transports() {
                let name = transport.name().to_string();
                let (endpoint, accept) = transport.listen().await;
                endpoints.insert(name.clone(), endpoint);
                accepts.insert(name, accept);
            }
            let receive: Receive = Box::new(move |upstream| {
                let serializer = self.spec.serializer(&upstream.serializer);
                // The listeners of the other transports close here.
                let accept = accepts.remove(&upstream.transport).unwrap();
                let input = self.clone();
                Box::pin(async move {
                    accept(Box::new(move |frame| {
                        let message: Frame = BincodeSerializer.deserialize(&frame);
                        deliver(&*serializer, upstream.delay, &input, &scheduler, message);
                    }))
                    .await;
                    self.close();
                })
            });
            (endpoints, receive)
        })
    }
}

impl<T: 'static> OutputEnd for OutputChannel<T> {
    fn describe(&self) -> PortInfo {
        self.spec.describe()
    }

    fn connect_to(self: Rc<Self>, downstream: Downstream) -> BoxFuture<()> {
        let serializer = self.spec.serializer(&downstream.serializer);
        let transport = self.spec.transport(&downstream.transport);
        let (frames, queued) = unbounded::<Vec<u8>>();
        self.senders
            .borrow_mut()
            .push(Box::new(move |timestamp, ent, physical, value| {
                let message = Frame {
                    timestamp,
                    ent,
                    physical,
                    value: value.map(|value| serializer.serialize(value)),
                };
                frames
                    .unbounded_send(BincodeSerializer.serialize(&message))
                    .unwrap();
            }));
        transport.send(downstream.endpoint, queued)
    }
}

/// Stores the node graph and launches one process per node.
pub struct App {
    connections: Vec<Connection>,
    nodes: Vec<Runner>,
    /// Every input port must have exactly one upstream connection.
    inputs: Vec<(NodeId, Rc<dyn InputEnd>)>,
}

impl App {
    pub fn new() -> Self {
        Self {
            connections: Vec::new(),
            nodes: Vec::new(),
            inputs: Vec::new(),
        }
    }

    /// Connect two ports with the first serializer and the first transport that both offer.
    pub fn connect<T: 'static>(&mut self, from: &OutputPort<T>, to: &InputPort<T>) {
        let common = |kind: &str, offered: Vec<String>, accepted: Vec<String>| {
            offered
                .into_iter()
                .find(|name| accepted.contains(name))
                .unwrap_or_else(|| {
                    panic!(
                        "output port {:?} and input port {:?} offer no common {kind}",
                        from.id, to.id
                    )
                })
        };
        let (output, input) = (from.channel.spec.describe(), to.channel.spec.describe());
        let serializer = common("serializer", output.serializers, input.serializers);
        let transport = common("transport", output.transports, input.transports);
        self.connect_with(from, to, &serializer, &transport, Time::ZERO);
    }

    pub fn connect_with<T: 'static>(
        &mut self,
        from: &OutputPort<T>,
        to: &InputPort<T>,
        serializer: &str,
        transport: &str,
        delay: Time,
    ) {
        let connection = Connection {
            from: PortRef {
                node_id: from.id.node_id,
                port: from.id.port_id,
            },
            to: PortRef {
                node_id: to.id.node_id,
                port: to.id.port_id,
            },
            delay,
            serializer: serializer.to_string(),
            transport: transport.to_string(),
        };
        assert!(
            self.connections
                .iter()
                .all(|other| other.to != connection.to),
            "input port {:?} already has an upstream; an input accepts exactly one connection",
            to.id
        );
        // Both ports must have the serializer and the transport.
        from.channel.spec.serializer(serializer);
        to.channel.spec.serializer(serializer);
        from.channel.spec.transport(transport);
        to.channel.spec.transport(transport);
        self.connections.push(connection);
    }

    /// Build a node; the process assigned to it runs this same node.
    pub fn create_node<N: 'static>(
        &mut self,
        constructor: impl FnOnce(&mut Builder) -> N,
        starts: Vec<Start<N>>,
    ) -> CreatedNode<N> {
        let node_id: NodeId = self.nodes.len() as NodeId;
        let created = CreatedNode::new(node_id, constructor, starts);
        let inputs = created.inputs.iter().cloned();
        self.inputs.extend(inputs.map(|input| (node_id, input)));
        self.nodes.push(created.runner.clone());
        created
    }

    pub fn run(self) {
        self.check_wiring();
        match Assignment::from_args() {
            Some(assignment) => {
                let runner = self.nodes.into_iter().nth(assignment.node_id).unwrap();
                run(&runner, assignment);
            }
            None => {
                let graph = Graph {
                    nodes: self.nodes.len(),
                    connections: self.connections,
                };
                let exe = std::env::current_exe().unwrap();
                let args: Vec<String> = std::env::args().skip(1).collect();
                Runtime::new().block_on(async move {
                    let coordinator = Coordinator::new(TcpTransport, BincodeSerializer).await;
                    let endpoint = coordinator.endpoint().to_string();
                    let children = (0..graph.nodes)
                        .map(|node_id| {
                            let assignment = Assignment {
                                node_id,
                                coordinator_endpoint: endpoint.clone(),
                            };
                            // The program's own arguments keep their positions.
                            std::process::Command::new(&exe)
                                .args(&args)
                                .args(assignment.args())
                                .spawn()
                                .unwrap()
                        })
                        .collect::<Vec<_>>();
                    let wired = coordinator.run(graph).await;
                    for mut child in children {
                        let status = child.wait().unwrap();
                        assert!(
                            wired.is_err() || status.success(),
                            "node exited with {status} status"
                        );
                    }
                    if let Err(refusal) = wired {
                        panic!("{refusal}");
                    }
                });
            }
        }
    }

    /// Assert that every input port has an upstream.
    fn check_wiring(&self) {
        for (node_id, input) in &self.inputs {
            let port = PortRef {
                node_id: *node_id,
                port: input.describe().id,
            };
            assert!(
                self.connections
                    .iter()
                    .any(|connection| connection.to == port),
                "input {port} has no upstream; connect it with App::connect"
            );
        }
    }
}

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
    inputs: Vec<Rc<dyn InputEnd>>,
    runner: Runner,
}

impl<N> Deref for CreatedNode<N> {
    type Target = N;

    fn deref(&self) -> &N {
        &self.node
    }
}

impl<N: 'static> CreatedNode<N> {
    fn new(
        node_id: NodeId,
        constructor: impl FnOnce(&mut Builder) -> N,
        starts: Vec<Start<N>>,
    ) -> Self {
        let runtime = Rc::new(Runtime::new());
        let scheduler = Rc::new(NodeScheduler::new(runtime.clone()));
        let mut builder = Builder::new(node_id, scheduler.clone());
        let node = Rc::new(constructor(&mut builder));
        let inputs = builder.inputs.clone();
        let running = node.clone();
        // Wire the node through the coordinator and run it until it finishes.
        let run = move |assignment: Assignment| {
            for task in builder.tasks {
                runtime.spawn_internal(task);
            }
            let frame_transfer = runtime.block_on(connect(
                scheduler.clone(),
                assignment,
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
            inputs,
            runner: Rc::new(RefCell::new(Some(Box::new(run)))),
        }
    }
}

fn run(runner: &Runner, assignment: Assignment) {
    let run = runner.take().expect("the node has already run");
    run(assignment);
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
    run(&node.runner, assignment);
}

/// Register the node's ports and return the task that transfers frames over all connections.
async fn connect(
    scheduler: Rc<NodeScheduler>,
    assignment: Assignment,
    inputs: Vec<Rc<dyn InputEnd>>,
    outputs: Vec<Rc<dyn OutputEnd>>,
) -> BoxFuture<()> {
    let mut ports = Vec::new();
    let mut listeners = HashMap::new();
    for input in &inputs {
        let (endpoints, receive) = input.clone().listen(scheduler.clone()).await;
        let port = PortInfo {
            endpoints: Some(endpoints),
            ..input.describe()
        };
        listeners.insert(port.id, receive);
        ports.push(port);
    }
    ports.extend(outputs.iter().map(|output| output.describe()));

    let wiring = register(
        &TcpTransport,
        &BincodeSerializer,
        &assignment.coordinator_endpoint,
        assignment.node_id,
        ports,
    )
    .await
    .unwrap_or_else(|refusal| panic!("node {}: {refusal}", assignment.node_id));
    scheduler.start();

    let receives: Vec<_> = wiring
        .upstream
        .into_iter()
        .map(|(id, upstream)| listeners.remove(&id).unwrap()(upstream))
        .collect();
    let mut sends = Vec::new();
    for output in &outputs {
        let id = output.describe().id;
        for downstream in wiring.downstream.get(&id).into_iter().flatten() {
            sends.push(output.clone().connect_to(downstream.clone()));
        }
    }
    Box::pin(async move {
        futures::join!(join_all(sends), join_all(receives));
    })
}

/// Package async node methods as startup tasks for `create_node`.
#[macro_export]
macro_rules! register {
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
    use super::*;
    use crate::node::EventSource;
    use crate::serializer::tests::Decimal;
    use crate::time::{MS, S};
    use crate::transport::Transport;
    use crate::transport::tests::Loopback;

    /// Create a node at logical time 5s for edge-delivery tests.
    fn node_at_5s() -> (Rc<NodeScheduler>, InputChannel<i64>) {
        let scheduler = Rc::new(NodeScheduler::new(Rc::new(Runtime::new())));
        scheduler.advance_to(5 * S);
        (scheduler, InputChannel::new())
    }

    fn receive(
        scheduler: &NodeScheduler,
        channel: &InputChannel<i64>,
        timestamp: Time,
        ent: Time,
        physical: bool,
    ) {
        let message = Frame {
            timestamp,
            ent,
            physical,
            value: Some(BincodeSerializer.serialize(&7i64)),
        };
        deliver(&BincodeSerializer, 10 * MS, channel, scheduler, message);
    }

    #[test]
    fn a_logical_frame_keeps_its_tag_plus_the_delay() {
        let (scheduler, channel) = node_at_5s();
        receive(&scheduler, &channel, 7 * S, 6 * S, false);
        assert_eq!(channel.next_event_tag(), Some(7 * S + 10 * MS));
        assert_eq!(channel.ent(), 6 * S + 10 * MS);
    }

    #[test]
    fn a_physical_frame_from_this_nodes_past_is_moved_right_after_now() {
        let (scheduler, channel) = node_at_5s();
        // The delayed sender tag is behind this node's clock.
        receive(&scheduler, &channel, 1 * S, 6 * S, true);
        assert_eq!(channel.next_event_tag(), Some((5 * S).next_microstep()));
        // Retagging a physical value does not change its ENT promise.
        assert_eq!(channel.ent(), 6 * S + 10 * MS);
    }

    #[test]
    fn a_physical_frame_keeps_the_senders_tag_plus_the_delay() {
        let (scheduler, channel) = node_at_5s();
        receive(&scheduler, &channel, 7 * S, 20 * S, true);
        assert_eq!(channel.next_event_tag(), Some(7 * S + 10 * MS));
    }

    #[test]
    fn an_edge_delivers_in_the_order_it_was_sent() {
        let (scheduler, channel) = node_at_5s();
        // A late physical value moves to (5s, 1).
        receive(&scheduler, &channel, 1 * S, 6 * S, true);
        // The next late value follows at (5s, 2).
        receive(&scheduler, &channel, 2 * S, 6 * S, true);
        // The logical value follows both physical values.
        receive(&scheduler, &channel, 6 * S, 20 * S, false);
        let after_now = (5 * S).next_microstep();
        for expected in [after_now, after_now.next_microstep(), 6 * S + 10 * MS] {
            assert_eq!(channel.next_event_tag(), Some(expected));
            channel.wake_wakers(expected);
        }
        assert_eq!(channel.next_event_tag(), None);
    }

    #[test]
    #[should_panic(expected = "input port 0 of node 0 has no upstream")]
    fn an_unconnected_input_is_a_wiring_bug() {
        let mut app = App::new();
        let _input: CreatedNode<InputPort<i64>> =
            app.create_node(|builder| builder.input(), Vec::new());
        app.check_wiring();
    }

    /// Create an app with a source and a sink node, and return their ports.
    fn pair() -> (
        App,
        CreatedNode<OutputPort<i64>>,
        CreatedNode<InputPort<i64>>,
    ) {
        let mut app = App::new();
        let out = app.create_node(|builder| builder.output(), Vec::new());
        let input = app.create_node(|builder| builder.input(), Vec::new());
        (app, out, input)
    }

    #[test]
    fn a_connection_records_its_ports_and_what_they_have_in_common() {
        let (mut app, out, input) = pair();
        app.connect(&out, &input);
        app.check_wiring();
        let connection = &app.connections[0];
        assert_eq!(connection.from.to_string(), "port 0 of node 0");
        assert_eq!(connection.to.to_string(), "port 0 of node 1");
        assert_eq!(connection.serializer, "bincode");
        assert_eq!(connection.transport, "tcp");
        assert_eq!(connection.delay, Time::ZERO);
    }

    #[test]
    fn one_output_may_feed_several_inputs() {
        let (mut app, out, input) = pair();
        let other: CreatedNode<InputPort<i64>> =
            app.create_node(|builder| builder.input(), Vec::new());
        app.connect(&out, &input);
        app.connect_with(&out, &other, "bincode", "tcp", 10 * MS);
        app.check_wiring();
        assert_eq!(app.connections.len(), 2);
    }

    #[test]
    #[should_panic(expected = "already has an upstream")]
    fn a_second_upstream_is_a_wiring_bug() {
        let (mut app, out, input) = pair();
        app.connect(&out, &input);
        app.connect(&out, &input);
    }

    #[test]
    #[should_panic(expected = "has no serializer named \"pickle\"")]
    fn a_serializer_no_port_has_is_a_wiring_bug() {
        let (mut app, out, input) = pair();
        app.connect_with(&out, &input, "pickle", "tcp", Time::ZERO);
    }

    #[test]
    #[should_panic(expected = "has no transport named \"udp\"")]
    fn a_transport_no_port_has_is_a_wiring_bug() {
        let (mut app, out, input) = pair();
        app.connect_with(&out, &input, "bincode", "udp", Time::ZERO);
    }

    #[test]
    #[should_panic(expected = "offer no common transport")]
    fn ports_without_a_common_transport_cannot_connect() {
        let (mut app, out, input) = pair();
        out.set_transport(Loopback);
        app.connect(&out, &input);
    }

    #[test]
    fn a_connection_uses_the_serializer_and_transport_its_ports_were_set_to() {
        let port = |node_id| PortRef { node_id, port: 0 };
        let graph = Graph {
            nodes: 2,
            connections: vec![Connection {
                from: port(0),
                to: port(1),
                delay: Time::ZERO,
                serializer: Decimal.name().to_string(),
                transport: Transport::name(&Loopback).to_string(),
            }],
        };
        let (announce, announced) = std::sync::mpsc::channel();
        let coordinator = std::thread::spawn(move || {
            Runtime::new().block_on(async move {
                let coordinator = Coordinator::new(TcpTransport, BincodeSerializer).await;
                announce.send(coordinator.endpoint().to_string()).unwrap();
                coordinator.run(graph).await
            })
        });
        let coordinator_endpoint = announced.recv().unwrap();
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
