//! A diagram of an app in the style of Lingua Franca: each node is a box with its ports on its
//! sides, their names beside them and its tasks in the middle, and each connection is a line.
//! elkrs places the nodes and routes the connections; `svg` draws the result.

mod svg;

use std::path::Path;

use serde_json::{Value, json};

use crate::coordinator::Connection;
use crate::time::Time;
use svg::{ACTION, LABEL_SIZE, PORT, TASK_HEIGHT, TITLE_SIZE};

/// The space between a node's border and what it holds, and between the things it holds.
const PADDING: f64 = 6.0;
/// The height that the name of a node takes at the top of its box.
const TITLE_HEIGHT: f64 = PADDING + TITLE_SIZE + PADDING;
/// The distance from one port to the next on the same side of a node.
const PORT_PITCH: f64 = 16.0;
/// How far a port reaches into the box of its node.
const PORT_INSET: f64 = 3.3;

/// What a diagram shows of an app.
pub struct Diagram {
    pub nodes: Vec<DiagramNode>,
    pub connections: Vec<Connection>,
}

/// What a diagram shows of one node; the node's id is its position among the nodes.
#[derive(Clone)]
pub struct DiagramNode {
    pub name: String,
    /// The name and the kind of each port, in the order of their positions.
    pub ports: Vec<(String, PortKind)>,
    pub tasks: Vec<String>,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum PortKind {
    Input,
    Output,
    Action,
}

/// Estimate the width of a text; no font is at hand to measure it.
fn text_width(text: &str, size: f64) -> f64 {
    0.6 * size * text.chars().count() as f64
}

impl DiagramNode {
    fn ports_of(&self, kind: PortKind) -> impl Iterator<Item = (usize, &str)> {
        let ports = self.ports.iter().enumerate();
        ports
            .filter(move |(_, port)| port.1 == kind)
            .map(|(position, port)| (position, port.0.as_str()))
    }

    /// The width of the box of each task, which all tasks of a node share.
    fn task_width(&self) -> f64 {
        let widest = self.tasks.iter().map(|task| text_width(task, LABEL_SIZE));
        widest.fold(45.0, f64::max) + 4.0 * PADDING
    }

    /// The width of an action with its name beside it.
    fn action_width(name: &str) -> f64 {
        ACTION + PADDING / 2.0 + text_width(name, LABEL_SIZE)
    }

    /// The width of the column of actions and tasks in the middle of the node.
    fn column_width(&self) -> f64 {
        let actions = self.ports_of(PortKind::Action);
        let actions = actions.map(|(_, name)| Self::action_width(name));
        let tasks = (!self.tasks.is_empty()).then(|| self.task_width());
        actions.chain(tasks).fold(0.0, f64::max)
    }

    fn column_height(&self) -> f64 {
        let actions = self.ports_of(PortKind::Action).count() as f64 * (ACTION + PADDING);
        let rows = actions + self.tasks.len() as f64 * (TASK_HEIGHT + PADDING);
        (rows - PADDING).max(0.0)
    }

    /// The width that the names of the ports of one side take inside the node, with the space
    /// that keeps them off the column.
    fn names_width(&self, kind: PortKind) -> f64 {
        let names = self.ports_of(kind);
        let widest = names.map(|(_, name)| text_width(name, LABEL_SIZE) + PADDING);
        widest.fold(0.0, f64::max)
    }

    /// The height of the node below its name, where the column is and the ports on its sides.
    fn body_height(&self) -> f64 {
        let sides = [PortKind::Input, PortKind::Output].map(|kind| self.ports_of(kind).count());
        let ports = sides[0].max(sides[1]) as f64 * PORT_PITCH;
        self.column_height().max(ports)
    }

    /// The size of the node's box. It is wide enough for what its body holds side by side, the
    /// names of its inputs, the column, and the names of its outputs. And it is wide enough for
    /// its name, with its id in one corner and as much room in the other.
    fn size(&self, node_id: usize) -> (f64, f64) {
        let names = self.names_width(PortKind::Input) + self.names_width(PortKind::Output);
        let corner = text_width(&node_id.to_string(), LABEL_SIZE) + PADDING;
        let title = text_width(&self.name, TITLE_SIZE) + 2.0 * corner;
        let width = (names + self.column_width()).max(title);
        let body = match self.body_height() {
            0.0 => 0.0,
            body => body + PADDING,
        };
        (width + 2.0 * PADDING, TITLE_HEIGHT + body)
    }
}

impl Diagram {
    /// Lay the diagram out and write it to an SVG file.
    pub fn write(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        std::fs::write(&path, self.to_svg())
    }

    /// Lay the diagram out and return it as SVG.
    pub fn to_svg(&self) -> String {
        let laid_out = elkrs::create_elk().layout_json(&self.graph().to_string());
        let laid_out =
            laid_out.unwrap_or_else(|error| panic!("cannot lay out the diagram: {error}"));
        svg::draw(self, &laid_out)
    }

    /// Describe the diagram as a graph for elkrs: the nodes with their sizes and the places of
    /// their ports, and the connections.
    fn graph(&self) -> Value {
        let nodes = self.nodes.iter().enumerate().map(|(node_id, node)| {
            let (width, height) = node.size(node_id);
            let side = |kind: PortKind, x: f64| {
                let ports: Vec<_> = node.ports_of(kind).collect();
                // The ports of a side are centered on the body of their node.
                let ports_height = ports.len() as f64 * PORT_PITCH;
                let top = TITLE_HEIGHT + (node.body_height() - ports_height) / 2.0;
                let ports = ports.into_iter().enumerate();
                ports.map(move |(row, (position, _))| {
                    json!({
                        "id": format!("n{node_id}.p{position}"),
                        "x": x,
                        "y": top + row as f64 * PORT_PITCH + (PORT_PITCH - PORT) / 2.0,
                        "width": PORT,
                        "height": PORT,
                    })
                })
            };
            let inputs = side(PortKind::Input, PORT_INSET - PORT);
            let ports: Vec<Value> = inputs
                .chain(side(PortKind::Output, width - PORT_INSET))
                .collect();
            json!({
                "id": format!("n{node_id}"),
                "width": width,
                "height": height,
                "ports": ports,
                "layoutOptions": {"elk.portConstraints": "FIXED_POS"},
            })
        });
        let connections = self.connections.iter().enumerate();
        let edges = connections.map(|(index, connection)| {
            let end =
                |port: crate::coordinator::PortId| format!("n{}.p{}", port.node_id, port.port_id);
            // A delay is written on its connection.
            let delay = (connection.delay > Time::ZERO).then(|| {
                let delay = connection.delay.to_string();
                json!({
                    "id": format!("e{index}.delay"),
                    "width": text_width(&delay, LABEL_SIZE) + 2.0 * PADDING,
                    "height": LABEL_SIZE + PADDING,
                    "text": delay,
                    "layoutOptions": {"elk.edgeLabels.inline": "true"},
                })
            });
            json!({
                "id": format!("e{index}"),
                "sources": [end(connection.from)],
                "targets": [end(connection.to)],
                // In a loop, a connection with a delay is the one that is drawn backwards.
                "layoutOptions": {"elk.layered.priority.direction": i32::from(delay.is_none())},
                "labels": delay.into_iter().collect::<Vec<_>>(),
            })
        });
        json!({
            "id": "app",
            "children": nodes.collect::<Vec<_>>(),
            "edges": edges.collect::<Vec<_>>(),
            "layoutOptions": {
                "elk.algorithm": "layered",
                "elk.direction": "RIGHT",
                "elk.layered.nodePlacement.strategy": "NETWORK_SIMPLEX",
                "elk.padding": "[top=12,left=12,bottom=12,right=12]",
            },
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::coordinator::PortId;
    use crate::time::MS;

    fn node(name: &str, ports: &[(&str, PortKind)], tasks: &[&str]) -> DiagramNode {
        DiagramNode {
            name: name.to_string(),
            ports: ports
                .iter()
                .map(|(name, kind)| (name.to_string(), *kind))
                .collect(),
            tasks: tasks.iter().map(|task| task.to_string()).collect(),
        }
    }

    fn connection(from: (usize, usize), to: (usize, usize), delay: Time) -> Connection {
        let end = |(node_id, port_id)| PortId { node_id, port_id };
        Connection {
            from: end(from),
            to: end(to),
            delay,
            physical: false,
        }
    }

    /// A camera whose image feeds a planner and, 10ms later, a logger that also takes the plan
    /// and the keys that a thread reads.
    pub(crate) fn robot() -> Diagram {
        use PortKind::{Action, Input, Output};
        Diagram {
            nodes: vec![
                node("Camera", &[("image", Output)], &["capture"]),
                node(
                    "Planner",
                    &[("image", Input), ("plan", Output)],
                    &["plan", "report"],
                ),
                node(
                    "Logger",
                    &[("plan", Input), ("keys", Action), ("image", Input)],
                    &["log"],
                ),
            ],
            connections: vec![
                connection((0, 0), (1, 0), Time::ZERO),
                connection((0, 0), (2, 2), 10 * MS),
                connection((1, 1), (2, 0), Time::ZERO),
            ],
        }
    }

    #[test]
    fn a_node_is_as_wide_as_its_name_or_as_what_its_body_holds_side_by_side() {
        let lone = node("N", &[], &[]);
        // The name is centered between the node's id in one corner and as much room in the other.
        let corners = 2.0 * (text_width("7", LABEL_SIZE) + PADDING);
        let name = text_width("N", TITLE_SIZE) + corners + 2.0 * PADDING;
        assert_eq!(lone.size(7), (name, TITLE_HEIGHT));
        let ports = [("left", PortKind::Input), ("right", PortKind::Output)];
        let full = node("N", &ports, &["a_rather_long_task_name"]);
        let names = text_width("left", LABEL_SIZE) + text_width("right", LABEL_SIZE);
        // The names of the ports are inside the node, on either side of its tasks.
        let body = names + 2.0 * PADDING + full.task_width();
        assert_eq!(full.size(7).0, body + 2.0 * PADDING);
    }

    #[test]
    fn a_node_is_as_high_as_its_tasks_or_as_the_ports_of_one_side() {
        let tasks = node("N", &[("a", PortKind::Input)], &["first", "second"]);
        let column = 2.0 * TASK_HEIGHT + PADDING;
        assert_eq!(tasks.size(0).1, TITLE_HEIGHT + column + PADDING);
        let inputs = [
            ("a", PortKind::Input),
            ("b", PortKind::Input),
            ("c", PortKind::Input),
        ];
        let ports = node("N", &inputs, &[]);
        assert_eq!(ports.size(0).1, TITLE_HEIGHT + 3.0 * PORT_PITCH + PADDING);
    }

    #[test]
    fn the_graph_has_a_port_for_each_input_and_output_and_an_edge_for_each_connection() {
        let graph = robot().graph();
        let ports = |node: usize| -> Vec<&str> {
            let ports = graph["children"][node]["ports"].as_array().unwrap();
            ports
                .iter()
                .map(|port| port["id"].as_str().unwrap())
                .collect()
        };
        assert_eq!(ports(0), ["n0.p0"]);
        // The action is not on a side of its node; the inputs keep their positions.
        assert_eq!(ports(2), ["n2.p0", "n2.p2"]);
        let delayed = &graph["edges"][1];
        assert_eq!(delayed["sources"][0], "n0.p0");
        assert_eq!(delayed["targets"][0], "n2.p2");
        assert_eq!(delayed["labels"][0]["text"], "10ms");
        assert!(graph["edges"][0]["labels"].as_array().unwrap().is_empty());
    }

    #[test]
    fn a_diagram_draws_every_node_port_task_and_connection() {
        let svg = robot().to_svg();
        // A node has its name at its top and its id in its corner.
        let texts = [
            "Camera", "0", "Logger", "2", "capture", "report", "keys", "10ms",
        ];
        for text in texts {
            assert!(
                svg.contains(&format!(">{text}</text>")),
                "{text} is missing"
            );
        }
        assert_eq!(svg.matches("class=\"node\"").count(), 3);
        assert_eq!(svg.matches("class=\"port\"").count(), 5);
        assert_eq!(svg.matches("class=\"task\"").count(), 4);
        assert_eq!(svg.matches("class=\"action\"").count(), 1);
        assert_eq!(svg.matches("class=\"connection\"").count(), 3);
    }

    #[test]
    fn only_a_physical_connection_is_a_dashed_line() {
        assert_eq!(robot().to_svg().matches("stroke-dasharray").count(), 0);
        let mut diagram = robot();
        diagram.connections[2].physical = true;
        let svg = diagram.to_svg();
        assert_eq!(svg.matches("class=\"connection\"").count(), 3);
        assert_eq!(svg.matches("stroke-dasharray").count(), 1);
    }

    #[test]
    fn the_same_app_gives_the_same_diagram() {
        assert_eq!(robot().to_svg(), robot().to_svg());
    }

    #[test]
    fn a_name_cannot_break_the_markup() {
        let diagram = Diagram {
            nodes: vec![node("Buffer<Vec<u8>> & co", &[], &[])],
            connections: Vec::new(),
        };
        let svg = diagram.to_svg();
        assert!(svg.contains(">Buffer&lt;Vec&lt;u8&gt;&gt; &amp; co</text>"));
    }
}
