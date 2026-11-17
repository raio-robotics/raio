use std::rc::Rc;

use futures::StreamExt;
use futures::channel::mpsc::unbounded;

use super::{HEARTBEAT_PERIOD, SERIALIZER, TRANSPORT};
use crate::driver::Timer;
use crate::messages::{Code, CoordinatorMessage, NodeMessage, RegisterNode, Reply, RequestCode};
use crate::node::{NodeId, NodeScheduler};
use crate::runtime::{BoxFuture, Runtime};
use crate::serializer::Serializer;
use crate::transport::tcp::TcpTransport;
use crate::transport::{Receiver, Sender, Transport};

/// A node's connection to its coordinator, which the node opens by asking for its code.
pub struct Link {
    pub(crate) node_id: NodeId,
    /// The code the coordinator held for the node; a node written in Rust has none.
    pub code: Code,
    pub(super) sender: <TcpTransport as Transport>::Sender,
    pub(super) receiver: <TcpTransport as Transport>::Receiver,
}

impl Link {
    /// Connect to the coordinator and ask for the node's code.
    pub fn open(node_id: NodeId, coordinator_endpoint: &str) -> Self {
        let coordinator_endpoint = coordinator_endpoint.to_string();
        Runtime::new().block_on(async move {
            let (mut sender, receiver) = TRANSPORT.connect(&coordinator_endpoint).await;
            let request = SERIALIZER.serialize(&RequestCode { node_id });
            let code = SERIALIZER.deserialize(&sender.call(&request).await);
            Self {
                node_id,
                code,
                sender,
                receiver,
            }
        })
    }

    /// Register the node's ports; the reply comes once every node of the app has registered.
    pub(crate) async fn register(&mut self, registration: &RegisterNode) -> Reply {
        let request = SERIALIZER.serialize(registration);
        SERIALIZER.deserialize(&self.sender.call(&request).await)
    }

    /// Keep the link for as long as the node runs: send the node's heartbeat and its shutdown
    /// request to the coordinator, and bring the coordinator's messages to the node.
    pub(crate) fn run(self, scheduler: Rc<NodeScheduler>) -> BoxFuture<()> {
        let Link {
            mut sender,
            mut receiver,
            ..
        } = self;
        let (to_coordinator, mut outgoing) = unbounded::<NodeMessage>();
        let requests = to_coordinator.clone();
        *scheduler.shutdown_request.borrow_mut() = Some(Box::new(move |delay| {
            let request = NodeMessage::RequestShutdown(delay);
            requests.unbounded_send(request).unwrap();
        }));
        let heartbeats = to_coordinator.clone();
        Box::pin(async move {
            let send = async move {
                while let Some(message) = outgoing.next().await {
                    sender.send(&SERIALIZER.serialize(&message)).await;
                }
            };
            let heartbeat = async move {
                loop {
                    Timer::after(HEARTBEAT_PERIOD).await;
                    heartbeats.unbounded_send(NodeMessage::Heartbeat).unwrap();
                }
            };
            let receive = async move {
                while let Some(bytes) = receiver.recv().await {
                    match SERIALIZER.deserialize(&bytes) {
                        CoordinatorMessage::RequestNodeShutdown(delay) => {
                            let offer = NodeMessage::ShutdownTime(scheduler.offer_shutdown(delay));
                            to_coordinator.unbounded_send(offer).unwrap();
                        }
                        CoordinatorMessage::Shutdown(timestamp) => {
                            scheduler.shut_down_at(timestamp)
                        }
                        CoordinatorMessage::ShutdownNow => scheduler.shut_down_now(),
                    }
                }
                // A node that has lost its coordinator shuts down at once.
                scheduler.shut_down_now();
            };
            futures::join!(send, heartbeat, receive);
        })
    }
}
