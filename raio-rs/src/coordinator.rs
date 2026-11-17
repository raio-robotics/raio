use std::collections::BTreeMap;

use crate::app::Graph;
use crate::messages::{RegisterNode, Reply};
use crate::serializer::Serializer;
use crate::serializer::bincode::BincodeSerializer;
use crate::transport::tcp::TcpTransport;
use crate::transport::{Listener, Receiver, Sender, Transport};

/// The transport between nodes and their coordinator; both sides use this one.
pub const TRANSPORT: TcpTransport = TcpTransport;
/// The serializer of the messages between nodes and their coordinator.
pub const SERIALIZER: BincodeSerializer = BincodeSerializer;

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
            let registration: RegisterNode =
                self.serializer.deserialize(&receiver.recv().await.unwrap());
            let node_id = registration.node_id;
            assert!(
                registered.insert(node_id, registration).is_none(),
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
    registration: RegisterNode,
) -> Reply {
    let (mut sender, _) = transport.connect(coordinator_endpoint).await;
    let request = serializer.serialize(&registration);
    serializer.deserialize(&sender.call(&request).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::graph::tests::{fan_out, graph, registrations};
    use crate::messages::Wiring;
    use crate::runtime::Runtime;

    /// Use a separate thread and runtime to simulate each node process.
    fn spawn_node(
        coordinator: String,
        registration: RegisterNode,
    ) -> std::thread::JoinHandle<Reply> {
        std::thread::spawn(move || {
            Runtime::new().block_on(async move {
                register(&TRANSPORT, &SERIALIZER, &coordinator, registration).await
            })
        })
    }

    #[test]
    fn every_node_gets_its_wiring_once_all_have_registered() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::new(TRANSPORT, SERIALIZER).await;
            let endpoint = coordinator.endpoint().to_string();
            let children: Vec<_> = registrations()
                .into_iter()
                .map(|registration| spawn_node(endpoint.clone(), registration))
                .collect();
            assert_eq!(coordinator.run(graph(fan_out())).await, Ok(()));
            let wirings: Vec<Wiring> = children
                .into_iter()
                .map(|child| child.join().unwrap().unwrap())
                .collect();
            assert_eq!(wirings[0][&0].len(), 2);
            assert!(wirings[2].is_empty()); // The logger has no outputs.
        });
    }

    #[test]
    fn a_graph_that_does_not_fit_is_reported_to_every_node() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::new(TRANSPORT, SERIALIZER).await;
            let endpoint = coordinator.endpoint().to_string();
            let children: Vec<_> = registrations()
                .into_iter()
                .map(|registration| spawn_node(endpoint.clone(), registration))
                .collect();
            let refused = coordinator.run(graph(fan_out()[..1].to_vec())).await;
            assert_eq!(
                refused,
                Err("input port 3 of node 2 has no connection".to_string())
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
            let coordinator = Coordinator::new(TRANSPORT, SERIALIZER).await;
            let endpoint = coordinator.endpoint().to_string();
            for _ in 0..2 {
                let registration = RegisterNode {
                    node_id: 0,
                    inputs: Vec::new(),
                    outputs: Vec::new(),
                };
                spawn_node(endpoint.clone(), registration);
            }
            let two_nodes = Graph {
                nodes: 2,
                connections: Vec::new(),
            };
            let _ = coordinator.run(two_nodes).await; // Both child nodes register the same node ID.
        });
    }
}
