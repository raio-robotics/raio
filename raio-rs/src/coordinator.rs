use std::collections::HashMap;

use crate::messages::{EndpointTable, RegisterNode};
use crate::port::PortId;
use crate::serializer::Serializer;
use crate::transport::{Receiver, Sender, Transport};

pub struct Coordinator<T: Transport, S> {
    transport: T,
    serializer: S,
}

impl<T: Transport, S: Serializer<RegisterNode> + Serializer<EndpointTable>> Coordinator<T, S> {
    pub async fn new(serializer: S) -> Self {
        Self {
            transport: T::bind().await,
            serializer,
        }
    }

    pub fn endpoint(&self) -> &str {
        self.transport.endpoint()
    }

    /// Wait for all nodes to register, then send the complete endpoint table to each node.
    pub async fn run(mut self, nodes: usize) {
        let mut repliers = Vec::new();
        let mut registered = Vec::new();
        let mut table = EndpointTable::new();
        for _ in 0..nodes {
            let (replier, mut receiver) = self.transport.accept().await;
            let RegisterNode { node_id, endpoints } =
                self.serializer.deserialize(&receiver.recv().await.unwrap());
            assert!(
                !registered.contains(&node_id),
                "node {node_id} registered twice"
            );
            registered.push(node_id);
            for (port, endpoint) in endpoints {
                let previous = table.insert(port, endpoint);
                assert!(previous.is_none(), "port {port:?} registered twice");
            }
            repliers.push(replier);
        }
        let reply = self.serializer.serialize(&table);
        for replier in &mut repliers {
            replier.send(&reply).await;
        }
    }
}

/// Register this node's input addresses and wait for every node's addresses.
pub async fn register<T: Transport, S: Serializer<RegisterNode> + Serializer<EndpointTable>>(
    serializer: &S,
    coordinator_endpoint: &str,
    node_id: usize,
    endpoints: HashMap<PortId, String>,
) -> EndpointTable {
    let (mut sender, _) = T::connect(coordinator_endpoint).await;
    let request = serializer.serialize(&RegisterNode { node_id, endpoints });
    serializer.deserialize(&sender.call(&request).await)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::runtime::Runtime;
    use crate::serializer::bincode::BincodeSerializer;
    use crate::transport::tcp::TcpTransport;

    fn port(node_id: usize, port_id: usize) -> PortId {
        PortId { node_id, port_id }
    }

    /// Use a separate thread and runtime to simulate each node process.
    fn spawn_node(
        coordinator: String,
        node_id: usize,
        registered: Arc<AtomicUsize>,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            Runtime::new().block_on(async move {
                let transport = TcpTransport::bind().await;
                let endpoints =
                    HashMap::from([(port(node_id, 0), transport.endpoint().to_string())]);
                registered.fetch_add(1, Ordering::SeqCst);
                let table = register::<TcpTransport, _>(
                    &BincodeSerializer,
                    &coordinator,
                    node_id,
                    endpoints,
                )
                .await;
                // Registration completes only after all nodes have registered.
                assert_eq!(registered.load(Ordering::SeqCst), 3);
                assert_eq!(table.len(), 3);
                assert_eq!(table[&port(node_id, 0)], transport.endpoint());
            })
        })
    }

    #[test]
    fn barrier_and_table() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::<TcpTransport, _>::new(BincodeSerializer).await;
            let endpoint = coordinator.endpoint().to_string();
            let registered = Arc::new(AtomicUsize::new(0));
            let children: Vec<_> = (0..3)
                .map(|node_id| spawn_node(endpoint.clone(), node_id, registered.clone()))
                .collect();
            coordinator.run(3).await;
            for child in children {
                child.join().unwrap();
            }
        });
    }

    #[test]
    fn a_node_may_register_many_ports_or_none() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::<TcpTransport, _>::new(BincodeSerializer).await;
            let endpoint = coordinator.endpoint().to_string();
            let child = std::thread::spawn(move || {
                Runtime::new().block_on(async move {
                    let endpoints = HashMap::from([
                        (port(7, 0), "a".to_string()),
                        (port(7, 1), "b".to_string()),
                    ]);
                    let table =
                        register::<TcpTransport, _>(&BincodeSerializer, &endpoint, 7, endpoints)
                            .await;
                    assert_eq!(table.len(), 2);
                    assert_eq!(table[&port(7, 1)], "b");
                })
            });
            coordinator.run(1).await;
            child.join().unwrap();
        });
    }

    #[test]
    #[should_panic(expected = "node 0 registered twice")]
    fn duplicate_node_id_is_a_launch_bug() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::<TcpTransport, _>::new(BincodeSerializer).await;
            let endpoint = coordinator.endpoint().to_string();
            for _ in 0..2 {
                let endpoint = endpoint.clone();
                std::thread::spawn(move || {
                    Runtime::new().block_on(async move {
                        register::<TcpTransport, _>(
                            &BincodeSerializer,
                            &endpoint,
                            0,
                            HashMap::new(),
                        )
                        .await;
                    })
                });
            }
            coordinator.run(2).await; // Both child nodes register the same node ID.
        });
    }

    #[test]
    #[should_panic(expected = "registered twice")]
    fn duplicate_port_is_a_wiring_bug() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::<TcpTransport, _>::new(BincodeSerializer).await;
            let endpoint = coordinator.endpoint().to_string();
            for node_id in 0..2 {
                let endpoint = endpoint.clone();
                std::thread::spawn(move || {
                    Runtime::new().block_on(async move {
                        let transport = TcpTransport::bind().await;
                        // Both nodes register the same port ID.
                        let endpoints =
                            HashMap::from([(port(0, 0), transport.endpoint().to_string())]);
                        register::<TcpTransport, _>(
                            &BincodeSerializer,
                            &endpoint,
                            node_id,
                            endpoints,
                        )
                        .await;
                    })
                });
            }
            coordinator.run(2).await;
        });
    }
}
