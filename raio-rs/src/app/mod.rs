mod builder;
mod connection;
pub(crate) mod graph;
mod interrupt;
mod port;
mod run;
mod setting;

use std::collections::HashMap;
use std::rc::Rc;

pub use crate::action::{Action, ActionHandle};
use crate::coordinator::{self, Link};
pub use crate::node::{ExecutionMode, Node};
use crate::serializer::Serializer;
use crate::time::Time;
use crate::transport::{AnyTransport, Transport};
pub use builder::Builder;
pub use graph::{Connection, Graph};
pub use port::{InputPort, OutputPort, PortId};
use run::Runner;
pub use run::{Assignment, CreatedNode, Start, create_node, run_node, run_node_as, run_node_on};

/// Stores the node graph and launches one process per node.
#[derive(Default)]
pub struct App {
    connections: Vec<Connection>,
    nodes: Vec<Runner>,
}

impl App {
    pub fn new() -> Self {
        Self::default()
    }

    /// Connect an output to an input; the returned options replace the defaults.
    pub fn connect<'a, T: 'static>(
        &'a mut self,
        from: &'a OutputPort<T>,
        to: &'a InputPort<T>,
    ) -> ConnectionOptions<'a, T> {
        self.connections.push(Connection {
            from: from.channel.setting.id,
            to: to.channel.setting.id,
            delay: Time::ZERO,
        });
        ConnectionOptions {
            connection: self.connections.last_mut().unwrap(),
            from,
            to,
        }
    }

    /// Build a node; the process assigned to it runs this same node.
    pub fn create_node<N: 'static>(
        &mut self,
        constructor: impl FnOnce(&mut Builder) -> N,
        starts: Vec<Start<N>>,
    ) -> CreatedNode<N> {
        let created = CreatedNode::new(self.nodes.len(), constructor, starts);
        self.nodes.push(created.runner.clone());
        created
    }

    /// Run this process's node, or launch a process for each node and coordinate them.
    pub fn run(self) {
        match Assignment::from_args() {
            Some(assignment) => {
                interrupt::ignore_interrupt();
                self.nodes[assignment.node_id].run(Link::open(assignment))
            }
            None => launch(Graph {
                nodes: self.nodes.len(),
                connections: self.connections,
            }),
        }
    }
}

/// Start this program once for each node and wire the nodes through a coordinator.
fn launch(graph: Graph) {
    let exe = std::env::current_exe().unwrap();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let nodes = graph.nodes;
    // Every node is this program, so the coordinator holds no code for them.
    let (endpoint, coordinator, interrupt) = coordinator::spawn(graph, HashMap::new());
    // On ^C every node runs its shutdown function at once, and the app ends as it otherwise does.
    interrupt::on_interrupt(move || interrupt.shutdown_now());
    let children: Vec<_> = (0..nodes)
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
        .collect();
    let wired = coordinator
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
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
}

/// Sets the options of the connection that `App::connect` just made.
pub struct ConnectionOptions<'a, T> {
    connection: &'a mut Connection,
    from: &'a OutputPort<T>,
    to: &'a InputPort<T>,
}

impl<T> ConnectionOptions<'_, T> {
    /// Deliver each value at its tag plus this delay.
    pub fn delay(self, delay: Time) -> Self {
        self.connection.delay = delay;
        self
    }

    /// Use this serializer for the connection.
    pub fn serializer(self, serializer: impl Serializer<T> + 'static) -> Self {
        let serializer: Rc<dyn Serializer<T>> = Rc::new(serializer);
        let name = serializer.name().to_string();
        // The output keeps what its other connections use; the input has only this connection.
        let (output, input) = (&self.from.channel.setting, &self.to.channel.setting);
        output.serializers.add(name.clone(), serializer.clone());
        input.serializers.set(name, serializer);
        self
    }

    /// Use this transport for the connection.
    pub fn transport(self, transport: impl Transport + 'static) -> Self {
        let transport: Rc<dyn AnyTransport> = Rc::new(transport);
        let name = transport.name().to_string();
        let (output, input) = (&self.from.channel.setting, &self.to.channel.setting);
        output.transports.add(name.clone(), transport.clone());
        input.transports.set(name, transport);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serializer::tests::Decimal;
    use crate::time::MS;
    use crate::transport::Transport;
    use crate::transport::tests::Loopback;

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
    fn a_connection_records_its_ports() {
        let (mut app, out, input) = pair();
        app.connect(&out, &input);
        let connection = &app.connections[0];
        assert_eq!(connection.from.to_string(), "port 0 of node 0");
        assert_eq!(connection.to.to_string(), "port 0 of node 1");
        assert_eq!(connection.delay, Time::ZERO);
    }

    #[test]
    fn one_output_may_feed_several_inputs() {
        let (mut app, out, input) = pair();
        let other = app.create_node(|builder| builder.input::<i64>(), Vec::new());
        app.connect(&out, &input);
        app.connect(&out, &other).delay(10 * MS);
        assert_eq!(app.connections.len(), 2);
        assert_eq!(app.connections[1].delay, 10 * MS);
    }

    #[test]
    fn a_connection_hands_its_serializer_and_transport_to_both_ports() {
        let (mut app, out, input) = pair();
        app.connect(&out, &input)
            .serializer(Decimal)
            .transport(Loopback);
        let (decimal, loopback) = (Decimal.name(), Transport::name(&Loopback));
        // The output keeps its defaults for other connections; the input has only this one.
        let output = out.channel.setting.describe();
        assert_eq!(output.serializers, ["bincode", decimal]);
        assert_eq!(output.transports, ["tcp", loopback]);
        assert_eq!(input.channel.setting.serializers.names(), [decimal]);
        assert_eq!(input.channel.setting.transports.names(), [loopback]);
    }
}
