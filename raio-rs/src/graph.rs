use std::collections::BTreeMap;
use std::fmt;

use crate::messages::{Downstream, PortInfo, Upstream, Wiring};
use crate::node::NodeId;
use crate::time::Time;

/// A port of a node, by its position among the ports the node built.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PortRef {
    pub node_id: NodeId,
    pub port: usize,
}

impl fmt::Display for PortRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "port {} of node {}", self.port, self.node_id)
    }
}

/// A connection from an output port to an input port.
#[derive(Clone, Debug)]
pub struct Connection {
    pub from: PortRef,
    pub to: PortRef,
    pub delay: Time,
    pub serializer: String,
    pub transport: String,
}

/// The nodes of an app and the connections between their ports.
#[derive(Clone, Debug)]
pub struct Graph {
    pub nodes: usize,
    pub connections: Vec<Connection>,
}

impl Graph {
    /// Match the registered ports against the graph and return each node's wiring.
    pub(crate) fn wire(
        &self,
        nodes: &BTreeMap<NodeId, Vec<PortInfo>>,
    ) -> Result<BTreeMap<NodeId, Wiring>, String> {
        let find = |end: &PortRef, input: bool| {
            let kind = if input { "input" } else { "output" };
            let ports = nodes
                .get(&end.node_id)
                .ok_or_else(|| format!("the graph connects {end}, but no such node registered"))?;
            let mut same_kind = ports
                .iter()
                .filter(|port| port.endpoints.is_some() == input);
            same_kind
                .clone()
                .find(|port| port.id == end.port)
                .ok_or_else(|| {
                    let ids: Vec<_> = same_kind.by_ref().map(|port| port.id).collect();
                    format!(
                        "node {} has no {kind} port {}; its {kind} ports are {ids:?}",
                        end.node_id, end.port
                    )
                })
        };
        let mut wirings: BTreeMap<NodeId, Wiring> = nodes
            .keys()
            .map(|node_id| (*node_id, Wiring::default()))
            .collect();
        for connection in &self.connections {
            let from = find(&connection.from, false)?;
            let to = find(&connection.to, true)?;
            for (end, port) in [(&connection.from, from), (&connection.to, to)] {
                for (kind, chosen, offered) in [
                    ("serializer", &connection.serializer, &port.serializers),
                    ("transport", &connection.transport, &port.transports),
                ] {
                    if !offered.contains(chosen) {
                        return Err(format!(
                            "{end} does not offer the {kind} {chosen:?}; it offers {offered:?}"
                        ));
                    }
                }
            }
            let upstream = Upstream {
                delay: connection.delay,
                serializer: connection.serializer.clone(),
                transport: connection.transport.clone(),
            };
            let input = wirings.get_mut(&connection.to.node_id).unwrap();
            if input.upstream.insert(to.id, upstream).is_some() {
                return Err(format!(
                    "{} already has an upstream; an input accepts exactly one connection",
                    connection.to
                ));
            }
            let output = wirings.get_mut(&connection.from.node_id).unwrap();
            output
                .downstream
                .entry(from.id)
                .or_default()
                .push(Downstream {
                    endpoint: to.endpoints.as_ref().unwrap()[&connection.transport].clone(),
                    serializer: connection.serializer.clone(),
                    transport: connection.transport.clone(),
                });
        }
        for (node_id, ports) in nodes {
            for port in ports.iter().filter(|port| port.endpoints.is_some()) {
                if !wirings[node_id].upstream.contains_key(&port.id) {
                    return Err(format!(
                        "input port {} of node {node_id} has no upstream",
                        port.id
                    ));
                }
            }
        }
        Ok(wirings)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::time::MS;
    use std::collections::HashMap;

    fn input(id: usize, endpoint: &str) -> PortInfo {
        PortInfo {
            endpoints: Some(HashMap::from([("tcp".to_string(), endpoint.to_string())])),
            ..output(id)
        }
    }

    fn output(id: usize) -> PortInfo {
        PortInfo {
            id,
            serializers: vec!["bincode".to_string()],
            transports: vec!["tcp".to_string()],
            endpoints: None,
        }
    }

    fn connection(from: (NodeId, usize), to: (NodeId, usize)) -> Connection {
        Connection {
            from: PortRef {
                node_id: from.0,
                port: from.1,
            },
            to: PortRef {
                node_id: to.0,
                port: to.1,
            },
            delay: Time::ZERO,
            serializer: "bincode".to_string(),
            transport: "tcp".to_string(),
        }
    }

    /// A camera whose image feeds a planner and a logger; the planner also has a plan output.
    pub(crate) fn nodes() -> BTreeMap<NodeId, Vec<PortInfo>> {
        BTreeMap::from([
            (0, vec![output(0)]),
            (1, vec![input(0, "planner:1"), output(1)]),
            (2, vec![input(3, "logger:1")]),
        ])
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

        let to = |endpoint: &str| Downstream {
            endpoint: endpoint.to_string(),
            serializer: "bincode".to_string(),
            transport: "tcp".to_string(),
        };
        // The output feeds both inputs, in the order the graph lists them.
        assert_eq!(
            wirings[&0].downstream[&0],
            vec![to("planner:1"), to("logger:1")]
        );
        assert!(wirings[&0].upstream.is_empty());
        assert_eq!(wirings[&1].upstream[&0].delay, Time::ZERO);
        assert!(wirings[&1].downstream.is_empty()); // Nothing listens to the plan.
        // Ports are keyed by the id their node gave them.
        assert_eq!(wirings[&2].upstream[&3].delay, 10 * MS);
    }

    fn refusal(connections: Vec<Connection>, nodes: BTreeMap<NodeId, Vec<PortInfo>>) -> String {
        graph(connections).wire(&nodes).unwrap_err()
    }

    #[test]
    fn an_unknown_port_is_refused_with_the_ports_that_exist() {
        let mut connections = fan_out();
        connections[0].to.port = 5;
        assert_eq!(
            refusal(connections, nodes()),
            "node 1 has no input port 5; its input ports are [0]"
        );
    }

    #[test]
    fn a_connection_runs_from_an_output_to_an_input() {
        let mut connections = fan_out();
        connections.push(connection((1, 0), (2, 3)));
        assert_eq!(
            refusal(connections, nodes()),
            "node 1 has no output port 0; its output ports are [1]"
        );
    }

    #[test]
    fn a_serializer_must_be_offered_by_both_ends() {
        let mut connections = fan_out();
        connections[1].serializer = "pickle".to_string();
        assert_eq!(
            refusal(connections, nodes()),
            "port 0 of node 0 does not offer the serializer \"pickle\"; it offers [\"bincode\"]"
        );
    }

    #[test]
    fn a_transport_must_be_offered_by_both_ends() {
        let mut nodes = nodes();
        let logger = &mut nodes.get_mut(&2).unwrap()[0];
        logger.transports = vec!["udp".to_string()];
        assert_eq!(
            refusal(fan_out(), nodes),
            "port 3 of node 2 does not offer the transport \"tcp\"; it offers [\"udp\"]"
        );
    }

    #[test]
    fn a_connection_reaches_the_input_at_the_address_of_its_transport() {
        let mut nodes = nodes();
        for port in nodes.values_mut().flatten() {
            port.transports.push("udp".to_string());
            if let Some(endpoints) = &mut port.endpoints {
                endpoints.insert("udp".to_string(), format!("udp:{}", port.id));
            }
        }
        let mut connections = fan_out();
        connections[1].transport = "udp".to_string();
        let wirings = graph(connections).wire(&nodes).unwrap();
        let endpoints: Vec<_> = wirings[&0].downstream[&0]
            .iter()
            .map(|downstream| downstream.endpoint.as_str())
            .collect();
        assert_eq!(endpoints, ["planner:1", "udp:3"]);
        assert_eq!(wirings[&2].upstream[&3].transport, "udp");
    }

    #[test]
    fn an_input_needs_exactly_one_upstream() {
        assert_eq!(
            refusal(fan_out()[..1].to_vec(), nodes()),
            "input port 3 of node 2 has no upstream"
        );
        let mut twice = fan_out();
        twice.push(connection((1, 1), (2, 3)));
        assert_eq!(
            refusal(twice, nodes()),
            "port 3 of node 2 already has an upstream; an input accepts exactly one connection"
        );
    }

    #[test]
    fn a_connection_to_a_missing_node_is_refused() {
        let mut connections = fan_out();
        connections.push(connection((7, 0), (2, 3)));
        assert_eq!(
            refusal(connections, nodes()),
            "the graph connects port 0 of node 7, but no such node registered"
        );
    }
}
