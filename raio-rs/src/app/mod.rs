//! The building blocks of a node program: the ports, actions and reactions of a node, and the
//! `App` that connects nodes and launches them.

mod action;
mod builder;
mod connection;
mod interrupt;
mod launch;
mod port;
mod reaction;
mod run;
mod setting;

use std::path::Path;
use std::rc::Rc;

use crate::coordinator::{Connection, Graph, Link};
use crate::diagram::Diagram;
pub use crate::node::{ExecutionMode, Node};
use crate::serializer::Serializer;
use crate::time::Time;
use crate::transport::{AnyTransport, Transport};
pub use action::{Action, ActionHandle};
pub use builder::{Builder, name_ports};
pub use port::{InputPort, OutputPort};
pub use reaction::{Trigger, next_event};
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
            to: to.setting.id,
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
        tasks: Vec<Start<N>>,
    ) -> CreatedNode<N> {
        let created = CreatedNode::new(self.nodes.len(), constructor, tasks);
        self.nodes.push(created.runner.clone());
        created
    }

    /// Draw the app's nodes and connections to an SVG file, in the style of Lingua Franca.
    pub fn diagram(&self, path: impl AsRef<Path>) {
        // The process of each node runs this program too; only the launcher draws.
        if Assignment::from_args().is_some() {
            return;
        }
        let diagram = Diagram {
            nodes: self.nodes.iter().map(|node| node.diagram.clone()).collect(),
            connections: self.connections.clone(),
        };
        let written = diagram.write(&path);
        written.unwrap_or_else(|error| {
            panic!(
                "cannot write the diagram to {}: {error}",
                path.as_ref().display()
            )
        });
    }

    /// Run this process's node, or launch a process for each node and coordinate them.
    pub fn run(self) {
        match Assignment::from_args() {
            Some(assignment) => {
                interrupt::ignore_interrupt();
                let link = Link::open(assignment.node_id, &assignment.coordinator_endpoint);
                self.nodes[assignment.node_id].run(link)
            }
            None => launch::launch(Graph {
                nodes: self.nodes.len(),
                connections: self.connections,
            }),
        }
    }
}

/// Sets the options of the connection that `App::connect` just made.
pub struct ConnectionOptions<'a, T> {
    connection: &'a mut Connection,
    from: &'a OutputPort<T>,
    to: &'a InputPort<T>,
}

impl<T> ConnectionOptions<'_, T> {
    /// Deliver each value at its timestamp plus this delay.
    pub fn delay(self, delay: Time) -> Self {
        self.connection.delay = delay;
        self
    }

    /// Use this serializer for the connection.
    pub fn serializer(self, serializer: impl Serializer<T> + 'static) -> Self {
        let serializer: Rc<dyn Serializer<T>> = Rc::new(serializer);
        let name = serializer.name().to_string();
        // The output keeps what its other connections use; the input has only this connection.
        let (output, input) = (&self.from.channel.setting, &self.to.setting);
        output.serializers.add(name.clone(), serializer.clone());
        input.serializers.set(name, serializer);
        self
    }

    /// Use this transport for the connection.
    pub fn transport(self, transport: impl Transport + 'static) -> Self {
        let transport: Rc<dyn AnyTransport> = Rc::new(transport);
        let name = transport.name().to_string();
        let (output, input) = (&self.from.channel.setting, &self.to.setting);
        output.transports.add(name.clone(), transport.clone());
        input.transports.set(name, transport);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagram::PortKind;
    use crate::serializer::tests::Decimal;
    use crate::time::MS;
    use crate::transport::Transport;
    use crate::transport::tests::Loopback;
    use crate::{ports, tasks};

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

    /// A node whose fields only give its ports their names.
    #[allow(dead_code)]
    struct Camera {
        image: OutputPort<i64>,
        shutter: crate::app::Action<()>,
        exposure: InputPort<i64>,
    }

    impl Camera {
        async fn capture(self: Rc<Self>) {}
        async fn expose(self: Rc<Self>) {}
    }

    #[allow(dead_code)]
    struct Lens {
        light: OutputPort<i64>,
    }

    /// The name and the kind of each port that a diagram shows of the app's second node.
    fn shown_ports(app: &App) -> Vec<(&str, PortKind)> {
        let ports = app.nodes[1].diagram.ports.iter();
        ports.map(|(name, kind)| (name.as_str(), *kind)).collect()
    }

    #[test]
    fn a_diagram_names_a_node_after_its_type_and_its_tasks_and_ports_after_methods_and_fields() {
        let mut app = App::new();
        let constructor = |builder: &mut Builder| {
            ports!(Camera {
                image: builder.output(),
                shutter: builder.action(),
                exposure: builder.input(),
            })
        };
        app.create_node(|builder| builder.output::<i64>(), Vec::new());
        app.create_node(constructor, tasks![Camera::capture, Camera::expose]);
        let camera = &app.nodes[1].diagram;
        assert_eq!(camera.name, "Camera");
        assert_eq!(camera.tasks, ["capture", "expose"]);
        let ports = [
            ("image", PortKind::Output),
            ("shutter", PortKind::Action),
            ("exposure", PortKind::Input),
        ];
        assert_eq!(shown_ports(&app), ports);
    }

    #[test]
    fn a_port_that_was_given_no_name_is_shown_by_its_position() {
        let mut app = App::new();
        app.create_node(
            |builder| {
                ports!(Lens {
                    light: builder.output::<i64>()
                })
            },
            Vec::new(),
        );
        // The names end with the node whose fields gave them.
        app.create_node(|builder| builder.input::<i64>(), Vec::new());
        assert_eq!(shown_ports(&app), [("0", PortKind::Input)]);
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
        assert_eq!(input.setting.serializers.names(), [decimal]);
        assert_eq!(input.setting.transports.names(), [loopback]);
    }
}
