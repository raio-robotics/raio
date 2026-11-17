use std::collections::BTreeMap;

use super::port::PortId;
use crate::messages::{Downstream, RegisterNode, Wiring};
use crate::node::NodeId;
use crate::time::Time;

/// A connection from an output port to an input port.
#[derive(Clone, Debug)]
pub struct Connection {
    pub from: PortId,
    pub to: PortId,
    pub delay: Time,
}

/// The nodes of an app and the connections between their ports.
#[derive(Clone, Debug)]
pub struct Graph {
    pub nodes: usize,
    pub connections: Vec<Connection>,
}

impl Graph {
    /// Check the graph against the registered ports and return each node's wiring.
    ///
    /// A connection goes from an output to an input, the output has the serializer and the
    /// transport that the input uses, and every input has exactly one connection.
    pub(crate) fn wire(
        &self,
        nodes: &BTreeMap<NodeId, RegisterNode>,
    ) -> Result<BTreeMap<NodeId, Wiring>, String> {
        let mut wirings: BTreeMap<NodeId, Wiring> = nodes
            .keys()
            .map(|node_id| (*node_id, Wiring::default()))
            .collect();
        let mut connected = Vec::new();
        for connection in &self.connections {
            let (from, to) = (connection.from, connection.to);
            let output = nodes
                .get(&from.node_id)
                .and_then(|node| node.outputs.iter().find(|output| output.id == from.port_id))
                .ok_or_else(|| {
                    format!("node {} has no output port {}", from.node_id, from.port_id)
                })?;
            let input = nodes
                .get(&to.node_id)
                .and_then(|node| node.inputs.iter().find(|input| input.id == to.port_id))
                .ok_or_else(|| format!("node {} has no input port {}", to.node_id, to.port_id))?;
            for (kind, used, has) in [
                ("serializer", &input.serializer, &output.serializers),
                ("transport", &input.transport, &output.transports),
            ] {
                if !has.contains(used) {
                    return Err(format!(
                        "input {to} uses the {kind} {used:?}, but output {from} has {has:?}"
                    ));
                }
            }
            if connected.contains(&to) {
                return Err(format!(
                    "input {to} has two connections; an input takes exactly one"
                ));
            }
            connected.push(to);
            let wiring = wirings.get_mut(&from.node_id).unwrap();
            wiring.entry(from.port_id).or_default().push(Downstream {
                input: input.clone(),
                delay: connection.delay,
            });
        }
        for (node_id, node) in nodes {
            for input in &node.inputs {
                let input = PortId {
                    node_id: *node_id,
                    port_id: input.id,
                };
                if !connected.contains(&input) {
                    return Err(format!("input {input} has no connection"));
                }
            }
        }
        Ok(wirings)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::messages::{InputInfo, OutputInfo};
    use crate::time::MS;

    fn input(id: usize, endpoint: &str) -> InputInfo {
        InputInfo {
            id,
            serializer: "bincode".to_string(),
            transport: "tcp".to_string(),
            endpoint: endpoint.to_string(),
        }
    }

    fn output(id: usize) -> OutputInfo {
        OutputInfo {
            id,
            serializers: vec!["bincode".to_string()],
            transports: vec!["tcp".to_string()],
        }
    }

    /// A camera whose image feeds a planner and a logger; the planner also has a plan output.
    ///
    /// Each node's id is its position in the list.
    pub(crate) fn registrations() -> Vec<RegisterNode> {
        let register = |inputs, outputs| RegisterNode { inputs, outputs };
        vec![
            register(vec![], vec![output(0)]),
            register(vec![input(0, "planner:1")], vec![output(1)]),
            register(vec![input(3, "logger:1")], vec![]),
        ]
    }

    fn nodes() -> BTreeMap<NodeId, RegisterNode> {
        registrations().into_iter().enumerate().collect()
    }

    fn connection(from: (NodeId, usize), to: (NodeId, usize)) -> Connection {
        let end = |(node_id, port_id)| PortId { node_id, port_id };
        Connection {
            from: end(from),
            to: end(to),
            delay: Time::ZERO,
        }
    }

    pub(crate) fn graph(connections: Vec<Connection>) -> Graph {
        Graph {
            nodes: 3,
            connections,
        }
    }

    pub(crate) fn fan_out() -> Vec<Connection> {
        vec![connection((0, 0), (1, 0)), connection((0, 0), (2, 3))]
    }

    #[test]
    fn each_node_learns_only_its_own_connections() {
        let mut delayed = fan_out();
        delayed[1].delay = 10 * MS;
        let wirings = graph(delayed).wire(&nodes()).unwrap();

        let to = |input, delay| Downstream { input, delay };
        // The output feeds both inputs, in the order the graph lists them.
        assert_eq!(
            wirings[&0][&0],
            vec![
                to(input(0, "planner:1"), Time::ZERO),
                to(input(3, "logger:1"), 10 * MS)
            ]
        );
        assert!(wirings[&1].is_empty()); // Nothing listens to the plan.
        assert!(wirings[&2].is_empty()); // An input learns nothing from its wiring.
    }

    fn refusal(connections: Vec<Connection>, nodes: BTreeMap<NodeId, RegisterNode>) -> String {
        graph(connections).wire(&nodes).unwrap_err()
    }

    #[test]
    fn a_connection_to_a_port_that_does_not_exist_is_refused() {
        let mut connections = fan_out();
        connections[0].to.port_id = 5;
        assert_eq!(refusal(connections, nodes()), "node 1 has no input port 5");
    }

    #[test]
    fn a_connection_goes_from_an_output_to_an_input() {
        let mut connections = fan_out();
        // Port 0 of the planner is an input.
        connections.push(connection((1, 0), (2, 3)));
        assert_eq!(refusal(connections, nodes()), "node 1 has no output port 0");
    }

    #[test]
    fn a_connection_from_a_node_that_does_not_exist_is_refused() {
        let mut connections = fan_out();
        connections.push(connection((7, 0), (2, 3)));
        assert_eq!(refusal(connections, nodes()), "node 7 has no output port 0");
    }

    /// Set the logger's input to a serializer and a transport of its own.
    fn logger_on_msgpack_over_udp() -> BTreeMap<NodeId, RegisterNode> {
        let mut nodes = nodes();
        let logger = &mut nodes.get_mut(&2).unwrap().inputs[0];
        logger.serializer = "msgpack".to_string();
        logger.transport = "udp".to_string();
        nodes
    }

    #[test]
    fn an_output_needs_the_serializer_its_input_uses() {
        assert_eq!(
            refusal(fan_out(), logger_on_msgpack_over_udp()),
            "input port 3 of node 2 uses the serializer \"msgpack\", \
             but output port 0 of node 0 has [\"bincode\"]"
        );
    }

    #[test]
    fn an_output_needs_the_transport_its_input_uses() {
        let mut nodes = logger_on_msgpack_over_udp();
        let camera = &mut nodes.get_mut(&0).unwrap().outputs[0];
        camera.serializers.push("msgpack".to_string());
        assert_eq!(
            refusal(fan_out(), nodes),
            "input port 3 of node 2 uses the transport \"udp\", \
             but output port 0 of node 0 has [\"tcp\"]"
        );
    }

    #[test]
    fn each_connection_uses_the_serializer_and_transport_of_its_input() {
        let mut nodes = logger_on_msgpack_over_udp();
        let camera = &mut nodes.get_mut(&0).unwrap().outputs[0];
        camera.serializers.push("msgpack".to_string());
        camera.transports.push("udp".to_string());
        let wirings = graph(fan_out()).wire(&nodes).unwrap();
        let used: Vec<_> = wirings[&0][&0]
            .iter()
            .map(|downstream| &downstream.input)
            .map(|input| (input.serializer.as_str(), input.transport.as_str()))
            .collect();
        assert_eq!(used, [("bincode", "tcp"), ("msgpack", "udp")]);
    }

    #[test]
    fn an_input_without_a_connection_is_refused() {
        assert_eq!(
            refusal(fan_out()[..1].to_vec(), nodes()),
            "input port 3 of node 2 has no connection"
        );
    }

    #[test]
    fn a_second_connection_to_an_input_is_refused() {
        let mut twice = fan_out();
        twice.push(connection((1, 1), (2, 3)));
        assert_eq!(
            refusal(twice, nodes()),
            "input port 3 of node 2 has two connections; an input takes exactly one"
        );
    }
}
