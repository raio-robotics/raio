use std::collections::BTreeMap;

use crate::graph::Graph;
use crate::messages::{PortInfo, RegisterNode, Reply};
use crate::serializer::Serializer;
use crate::transport::{Listener, Receiver, Sender, Transport};

pub struct Coordinator<T: Transport, S> {
    listener: T::Listener,
    serializer: S,
}

impl<T: Transport, S: Serializer<RegisterNode> + Serializer<Reply>> Coordinator<T, S> {
    pub async fn new(transport: T, serializer: S) -> Self {
        Self {
            listener: transport.bind().await,
            serializer,
        }
    }

    pub fn endpoint(&self) -> &str {
        self.listener.endpoint()
    }

    /// Wait for all nodes to register, then send each its wiring or the reason for refusing it.
    pub async fn run(mut self, graph: Graph) -> Result<(), String> {
        let mut repliers = Vec::new();
        let mut registered = BTreeMap::new();
        for _ in 0..graph.nodes {
            let (replier, mut receiver) = self.listener.accept().await;
            let RegisterNode { node_id, ports } =
                self.serializer.deserialize(&receiver.recv().await.unwrap());
            assert!(
                registered.insert(node_id, ports).is_none(),
                "node {node_id} registered twice"
            );
            repliers.push((node_id, replier));
        }
        let mut wirings = graph.wire(&registered);
        for (node_id, mut replier) in repliers {
            let reply: Reply = match &mut wirings {
                Ok(wirings) => Ok(wirings.remove(&node_id).unwrap()),
                Err(error) => Err(error.clone()),
            };
            replier.send(&self.serializer.serialize(&reply)).await;
        }
        wirings.map(|_| ())
    }
}

/// Describe this node's ports to the coordinator and wait for their wiring.
pub async fn register<T: Transport, S: Serializer<RegisterNode> + Serializer<Reply>>(
    transport: &T,
    serializer: &S,
    coordinator_endpoint: &str,
    node_id: usize,
    ports: Vec<PortInfo>,
) -> Reply {
    let (mut sender, _) = transport.connect(coordinator_endpoint).await;
    let request = serializer.serialize(&RegisterNode { node_id, ports });
    serializer.deserialize(&sender.call(&request).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::tests::{fan_out, graph, nodes};
    use crate::messages::Wiring;
    use crate::runtime::Runtime;
    use crate::serializer::bincode::BincodeSerializer;
    use crate::transport::tcp::TcpTransport;

    /// Use a separate thread and runtime to simulate each node process.
    fn spawn_node(
        coordinator: String,
        node_id: usize,
        ports: Vec<PortInfo>,
    ) -> std::thread::JoinHandle<Reply> {
        std::thread::spawn(move || {
            Runtime::new().block_on(async move {
                let (transport, serializer) = (TcpTransport, BincodeSerializer);
                register(&transport, &serializer, &coordinator, node_id, ports).await
            })
        })
    }

    #[test]
    fn every_node_gets_its_wiring_once_all_have_registered() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::new(TcpTransport, BincodeSerializer).await;
            let endpoint = coordinator.endpoint().to_string();
            let children: Vec<_> = nodes()
                .into_iter()
                .map(|(node_id, ports)| spawn_node(endpoint.clone(), node_id, ports))
                .collect();
            assert_eq!(coordinator.run(graph(fan_out())).await, Ok(()));
            let wirings: Vec<Wiring> = children
                .into_iter()
                .map(|child| child.join().unwrap().unwrap())
                .collect();
            assert_eq!(wirings[0].downstream[&0].len(), 2);
            assert_eq!(wirings[2].upstream[&3].serializer, "bincode");
        });
    }

    #[test]
    fn a_graph_that_does_not_fit_is_reported_to_every_node() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::new(TcpTransport, BincodeSerializer).await;
            let endpoint = coordinator.endpoint().to_string();
            let children: Vec<_> = nodes()
                .into_iter()
                .map(|(node_id, ports)| spawn_node(endpoint.clone(), node_id, ports))
                .collect();
            let refused = coordinator.run(graph(fan_out()[..1].to_vec())).await;
            assert_eq!(
                refused,
                Err("input port 3 of node 2 has no upstream".to_string())
            );
            for child in children {
                assert_eq!(
                    child.join().unwrap(),
                    refused.clone().map(|_| Wiring::default())
                );
            }
        });
    }

    #[test]
    #[should_panic(expected = "node 0 registered twice")]
    fn duplicate_node_id_is_a_launch_bug() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::new(TcpTransport, BincodeSerializer).await;
            let endpoint = coordinator.endpoint().to_string();
            for _ in 0..2 {
                spawn_node(endpoint.clone(), 0, Vec::new());
            }
            let two_nodes = Graph {
                nodes: 2,
                connections: Vec::new(),
            };
            let _ = coordinator.run(two_nodes).await; // Both child nodes register the same node ID.
        });
    }
}
