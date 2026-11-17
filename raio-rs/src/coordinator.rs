use crate::messages::{EndpointTable, RegisterNode};
use crate::serializer::Serializer;
use crate::transport::{Receiver, Sender, Transport};

pub struct Coordinator<T: Transport, S> {
    transport: T,
    serializer: S,
}

impl<T: Transport, S: Serializer<RegisterNode> + Serializer<EndpointTable>> Coordinator<T, S> {
    pub async fn new(serializer: S) -> Self {
        Self {
            transport: T::new().await,
            serializer,
        }
    }

    pub fn endpoint(&self) -> &str {
        self.transport.endpoint()
    }

    /// Collect one registration per node, then broadcast the complete endpoint table to every node.
    pub async fn run(mut self, nodes: usize) {
        let mut repliers = Vec::new();
        let mut table = EndpointTable::new();
        for _ in 0..nodes {
            let (replier, mut receiver) = self.transport.accept().await;
            let RegisterNode { node_id, endpoint } =
                self.serializer.deserialize(&receiver.recv().await.unwrap());
            assert!(
                table.insert(node_id, endpoint).is_none(),
                "node {node_id} registered twice"
            );
            repliers.push(replier);
        }
        let reply = self.serializer.serialize(&table);
        for replier in &mut repliers {
            replier.send(&reply).await;
        }
    }
}

/// Nodes: announce this node's transport endpoint, then block until
/// every node has registered and the coordinator replies with the table.
pub async fn register<T: Transport, S: Serializer<RegisterNode> + Serializer<EndpointTable>>(
    transport: &T,
    serializer: &S,
    coordinator_endpoint: &str,
    node_id: usize,
) -> EndpointTable {
    let (mut sender, _) = transport.connect(coordinator_endpoint).await;
    let request = serializer.serialize(&RegisterNode {
        node_id,
        endpoint: transport.endpoint().to_string(),
    });
    serializer.deserialize(&sender.call(&request).await)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::runtime::Runtime;
    use crate::serializer::SerdeSerializer;
    use crate::transport::tcp::TcpTransport;

    /// Threads stand in for node processes: they share nothing but sockets,
    /// each with its own runtime — the same isolation real children have.
    fn spawn_node(
        coordinator: String,
        node_id: usize,
        registered: Arc<AtomicUsize>,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            Runtime::new().block_on(async move {
                let transport = TcpTransport::new().await;
                registered.fetch_add(1, Ordering::SeqCst);
                let table = register(&transport, &SerdeSerializer, &coordinator, node_id).await;
                // register() only returns once everyone is in: the barrier.
                assert_eq!(registered.load(Ordering::SeqCst), 3);
                assert_eq!(table.len(), 3);
                assert_eq!(table[&node_id], transport.endpoint());
            })
        })
    }

    #[test]
    fn barrier_and_table() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::<TcpTransport, _>::new(SerdeSerializer).await;
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
    fn single_node() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::<TcpTransport, _>::new(SerdeSerializer).await;
            let endpoint = coordinator.endpoint().to_string();
            let child = std::thread::spawn(move || {
                Runtime::new().block_on(async move {
                    let transport = TcpTransport::new().await;
                    let table = register(&transport, &SerdeSerializer, &endpoint, 7).await;
                    assert_eq!(table.len(), 1);
                    assert_eq!(table[&7], transport.endpoint());
                })
            });
            coordinator.run(1).await;
            child.join().unwrap();
        });
    }

    #[test]
    #[should_panic(expected = "registered twice")]
    fn duplicate_node_id_is_a_launch_bug() {
        Runtime::new().block_on(async {
            let coordinator = Coordinator::<TcpTransport, _>::new(SerdeSerializer).await;
            let endpoint = coordinator.endpoint().to_string();
            for _ in 0..2 {
                let endpoint = endpoint.clone();
                std::thread::spawn(move || {
                    Runtime::new().block_on(async move {
                        let transport = TcpTransport::new().await;
                        register(&transport, &SerdeSerializer, &endpoint, 0).await;
                    })
                });
            }
            coordinator.run(2).await; // both children claim node 0
        });
    }
}
