//! A diagram of an app in the style of Lingua Franca: each node is a box with its ports on its
//! sides, their names beside them and its tasks and reactions in the middle, and each
//! connection is a line. A task is a rectangle with rounded corners. A reaction is a box with a
//! point, and a line joins it to each port or action that triggers it. Each task and reaction
//! has its number in its corner. elkrs places the nodes and routes the connections; `svg` draws
//! the result.

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
/// The distance between the vertical lines that lead to two reactions from their triggers.
const TRIGGER_PITCH: f64 = 4.0;

/// What a diagram shows of an app.
pub struct Diagram {
    /// The nodes in the order of their ids, which count from 1.
    pub nodes: Vec<DiagramNode>,
    pub connections: Vec<Connection>,
}

/// What a diagram shows of one node.
#[derive(Clone)]
pub struct DiagramNode {
    pub name: String,
    /// The name and the kind of each port, in the order of their positions.
    pub ports: Vec<(String, PortKind)>,
    /// The tasks and reactions, in the order in which the node declares them; their numbers
    /// count from 1 in this order.
    pub tasks: Vec<DiagramTask>,
}

/// What a diagram shows of one task or reaction.
#[derive(Clone, PartialEq, Debug)]
pub struct DiagramTask {
    pub name: String,
    /// The positions of the ports and actions that trigger a reaction; `None` for a task.
    pub triggers: Option<Vec<usize>>,
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

    /// The width of the box of each task and reaction, which all of a node share.
    fn task_width(&self) -> f64 {
        let names = self.tasks.iter();
        let widest = names.map(|task| text_width(&task.name, LABEL_SIZE));
        widest.fold(45.0, f64::max) + 4.0 * PADDING
    }

    /// The width, between the names of the inputs and the column, that the lines from triggers
    /// to reactions take: the lines to one reaction meet in a vertical line of their own.
    fn triggers_width(&self) -> f64 {
        match self.tasks.iter().flat_map(|task| &task.triggers).count() {
            0 => 0.0,
            reactions => reactions as f64 * TRIGGER_PITCH + PADDING,
        }
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

    /// The size of the node's box. It is wide enough for what its body holds side by side: the
    /// names of its inputs, the lines to its reactions, the column, and the names of its
    /// outputs. And it is wide enough for its name, with its id in one corner and as much room
    /// in the other.
    fn size(&self, node_id: usize) -> (f64, f64) {
        let names = self.names_width(PortKind::Input) + self.names_width(PortKind::Output);
        let corner = text_width(&node_id.to_string(), LABEL_SIZE) + PADDING;
        let title = text_width(&self.name, TITLE_SIZE) + 2.0 * corner;
        let width = (names + self.triggers_width() + self.column_width()).max(title);
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
        let nodes = (1..).zip(&self.nodes).map(|(node_id, node)| {
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

    /// A node with these ports, and these tasks and reactions: a reaction has the positions
    /// of its triggers, and a task has `None`.
    fn node(
        name: &str,
        ports: &[(&str, PortKind)],
        tasks: &[(&str, Option<&[usize]>)],
    ) -> DiagramNode {
        let tasks = tasks.iter().map(|(name, triggers)| DiagramTask {
            name: name.to_string(),
            triggers: triggers.map(<[usize]>::to_vec),
        });
        DiagramNode {
            name: name.to_string(),
            ports: ports
                .iter()
                .map(|(name, kind)| (name.to_string(), *kind))
                .collect(),
            tasks: tasks.collect(),
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
    /// and the keys that a thread reads. The planner reacts to the image, and the logger to
    /// all three of what it takes.
    pub(crate) fn robot() -> Diagram {
        use PortKind::{Action, Input, Output};
        Diagram {
            nodes: vec![
                node("Camera", &[("image", Output)], &[("capture", None)]),
                node(
                    "Planner",
                    &[("image", Input), ("plan", Output)],
                    &[("plan", Some(&[0])), ("report", None)],
                ),
                node(
                    "Logger",
                    &[("plan", Input), ("keys", Action), ("image", Input)],
                    &[("log", Some(&[0, 1, 2]))],
                ),
            ],
            connections: vec![
                connection((1, 0), (2, 0), Time::ZERO),
                connection((1, 0), (3, 2), 10 * MS),
                connection((2, 1), (3, 0), Time::ZERO),
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
        let full = node("N", &ports, &[("a_rather_long_task_name", None)]);
        let names = text_width("left", LABEL_SIZE) + text_width("right", LABEL_SIZE);
        // The names of the ports are inside the node, on either side of its tasks.
        let body = names + 2.0 * PADDING + full.task_width();
        assert_eq!(full.size(7).0, body + 2.0 * PADDING);
    }

    #[test]
    fn a_node_is_wider_by_the_lines_from_the_triggers_of_its_reactions() {
        let ports = [("a", PortKind::Input), ("b", PortKind::Input)];
        let task = node("N", &ports, &[("run", None)]);
        let reaction = node("N", &ports, &[("run", Some(&[0, 1]))]);
        // The lines to the one reaction meet in one vertical line.
        let lines = TRIGGER_PITCH + PADDING;
        assert_eq!(reaction.size(1).0, task.size(1).0 + lines);
    }

    #[test]
    fn a_node_is_as_high_as_its_tasks_or_as_the_ports_of_one_side() {
        let tasks = node(
            "N",
            &[("a", PortKind::Input)],
            &[("first", None), ("second", None)],
        );
        let column = 2.0 * TASK_HEIGHT + PADDING;
        assert_eq!(tasks.size(1).1, TITLE_HEIGHT + column + PADDING);
        let inputs = [
            ("a", PortKind::Input),
            ("b", PortKind::Input),
            ("c", PortKind::Input),
        ];
        let ports = node("N", &inputs, &[]);
        assert_eq!(ports.size(1).1, TITLE_HEIGHT + 3.0 * PORT_PITCH + PADDING);
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
        // The camera is node 1 and the logger node 3.
        assert_eq!(ports(0), ["n1.p0"]);
        // The action is not on a side of its node; the inputs keep their positions.
        assert_eq!(ports(2), ["n3.p0", "n3.p2"]);
        let delayed = &graph["edges"][1];
        assert_eq!(delayed["sources"][0], "n1.p0");
        assert_eq!(delayed["targets"][0], "n3.p2");
        assert_eq!(delayed["labels"][0]["text"], "10ms");
        assert!(graph["edges"][0]["labels"].as_array().unwrap().is_empty());
    }

    #[test]
    fn a_diagram_draws_every_node_port_task_and_connection() {
        let svg = robot().to_svg();
        // A node has its name at its top and its id in its corner.
        let texts = [
            "Camera", "1", "Logger", "3", "capture", "report", "keys", "10ms",
        ];
        for text in texts {
            assert!(
                svg.contains(&format!(">{text}</text>")),
                "{text} is missing"
            );
        }
        assert_eq!(svg.matches("class=\"node\"").count(), 3);
        assert_eq!(svg.matches("class=\"port\"").count(), 5);
        // A task is a rectangle. A reaction is a box with a point, with a line from each of its
        // triggers: one to the planner's reaction, and three to the logger's.
        assert_eq!(svg.matches("<rect class=\"task\"").count(), 2);
        assert_eq!(svg.matches("<polygon class=\"reaction\"").count(), 2);
        assert_eq!(svg.matches("class=\"trigger\"").count(), 4);
        assert_eq!(svg.matches("class=\"action\"").count(), 1);
        assert_eq!(svg.matches("class=\"connection\"").count(), 3);
    }

    #[test]
    fn the_tasks_and_reactions_of_a_node_have_their_numbers_from_1_in_declared_order() {
        let svg = robot().to_svg();
        let texts = svg.lines().filter_map(|line| line.strip_suffix("</text>"));
        let texts: Vec<&str> = texts.map(|text| text.rsplit_once('>').unwrap().1).collect();
        // The planner is node 2; it declares its reaction before its task.
        let planner = texts.iter().position(|text| *text == "Planner").unwrap();
        let shown = ["2", "Planner", "1", "plan", "2", "report"];
        assert_eq!(texts[planner - 1..planner + 5], shown);
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
