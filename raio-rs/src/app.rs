use crate::coordinator::{Coordinator, register};
use crate::messages::TaggedMessage;
use crate::node::{NodeContext, NodeId};
pub use crate::port::{Builder, InputPort, OutputPort, PortId};
use crate::port::{InputChannel, OutputChannel, OutputSender};
use crate::runtime::Runtime;
use crate::serializer::SerdeSerializer;
use crate::serializer::Serializer;
use crate::time::Time;
use crate::transport::tcp::TcpTransport;
use crate::transport::{Receiver, Sender, Transport};
use futures::StreamExt;
use futures::channel::mpsc::{UnboundedReceiver, unbounded};
use futures::future::join_all;
use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

type Runner = Box<dyn FnOnce(Rc<Runtime>, Vec<Edge>, String)>;
pub(crate) type BoxFuture<T> = Pin<Box<dyn Future<Output = T>>>;
pub(crate) type ReceiverFactory = Box<dyn FnOnce(Box<dyn Fn(Vec<u8>)>) -> BoxFuture<()>>;
pub(crate) type ListenerFactory = Box<dyn FnOnce() -> BoxFuture<(String, ReceiverFactory)>>;
pub(crate) type SenderFactory =
    Box<dyn FnOnce(String, UnboundedReceiver<Vec<u8>>) -> BoxFuture<()>>;

pub struct Edge {
    pub(crate) from: PortId,
    pub(crate) to: PortId,
    pub(crate) serialize: Box<dyn Fn(&Rc<dyn Any>) -> Option<Vec<u8>>>,
    pub(crate) deliver: Box<dyn Fn(&Rc<dyn Any>, TaggedMessage)>,
    pub(crate) close: Box<dyn Fn(&Rc<dyn Any>)>,
    pub(crate) install_sender: Box<dyn Fn(&Rc<dyn Any>, OutputSender)>,
    pub(crate) listener_factory: ListenerFactory,
    pub(crate) sender_factory: SenderFactory,
}

pub struct App {
    edges: Vec<Edge>,
    nodes: Vec<Runner>,
}

impl App {
    pub fn new() -> Self {
        Self {
            edges: Vec::new(),
            nodes: Vec::new(),
        }
    }

    pub fn connect<T>(&mut self, from: &OutputPort<T>, to: &InputPort<T>)
    where
        T: serde::Serialize + serde::de::DeserializeOwned + 'static,
    {
        self.connect_with::<TcpTransport, T, SerdeSerializer>(
            from,
            to,
            SerdeSerializer,
            Time::ZERO,
        );
    }

    pub fn connect_with<Tr, T, S>(
        &mut self,
        from: &OutputPort<T>,
        to: &InputPort<T>,
        serializer: S,
        delay: Time,
    ) where
        Tr: Transport + 'static,
        T: 'static,
        S: Serializer<T> + 'static,
    {
        assert!(
            self.edges.iter().all(|edge| edge.to != to.id),
            "input port {:?} already has an upstream; an input accepts exactly one connection",
            to.id
        );
        let serializer = Rc::new(serializer);
        let ser = serializer.clone();
        let serialize = Box::new(move |cell: &Rc<dyn Any>| {
            let cell = cell.clone().downcast::<OutputChannel<T>>().unwrap();
            let taken = cell.cell.borrow_mut().take();
            taken.map(|value| ser.serialize(&value))
        });
        let install_sender = Box::new(|channel: &Rc<dyn Any>, sender: OutputSender| {
            channel
                .clone()
                .downcast::<OutputChannel<T>>()
                .unwrap()
                .install_sender(sender);
        });
        let deliver = Box::new(move |channel: &Rc<dyn Any>, message: TaggedMessage| {
            let channel = channel.clone().downcast::<InputChannel<T>>().unwrap();
            if let Some(bytes) = message.value {
                channel.push(message.timestamp + delay, serializer.deserialize(&bytes));
            }
            channel.update_remote_ent(message.ent + delay);
        });
        let close = Box::new(|channel: &Rc<dyn Any>| {
            channel
                .clone()
                .downcast::<InputChannel<T>>()
                .unwrap()
                .close();
        });
        let listener_factory: ListenerFactory = Box::new(|| {
            Box::pin(async {
                let mut transport = Tr::new().await;
                let endpoint = transport.endpoint().to_string();
                let receiver_factory: ReceiverFactory = Box::new(move |deliver| {
                    Box::pin(async move {
                        let (_sender, mut receiver) = transport.accept().await;
                        while let Some(frame) = receiver.recv().await {
                            deliver(frame);
                        }
                    })
                });
                (endpoint, receiver_factory)
            })
        });
        let sender_factory: SenderFactory = Box::new(|endpoint, mut frames| {
            Box::pin(async move {
                let (mut sender, _receiver) = Tr::connect(&endpoint).await;
                while let Some(frame) = frames.next().await {
                    sender.send(&frame).await;
                }
            })
        });
        self.edges.push(Edge {
            from: from.id,
            to: to.id,
            serialize,
            deliver,
            close,
            install_sender,
            listener_factory,
            sender_factory,
        });
    }

    pub fn create_node<N, F, Fut>(
        &mut self,
        constructor: impl Fn(&mut Builder) -> N + 'static,
        start: F,
    ) -> N
    where
        N: 'static,
        F: FnOnce(N) -> Fut + 'static,
        Fut: std::future::Future<Output = ()> + 'static,
    {
        let node_id: NodeId = self.nodes.len() as NodeId;
        let node_context = Rc::new(NodeContext::new(Rc::new(Runtime::new())));
        let mut builder = Builder::new(node_id, node_context);
        let handle = constructor(&mut builder);
        let ports = builder.port_count();
        self.nodes
            .push(Box::new(move |runtime, edges, coordinator_endpoint| {
                let node_context = Rc::new(NodeContext::new(runtime.clone()));
                let mut builder = Builder::new(node_id, node_context.clone());
                let node = constructor(&mut builder);
                assert!(
                    builder.port_count() == ports,
                    "node {node_id}: constructor created a different number of ports \
                 than on its first run; constructors must be deterministic"
                );
                let ports = std::mem::take(&mut builder.ports);
                runtime.block_on(run_node(
                    node_context,
                    node_id,
                    coordinator_endpoint,
                    edges,
                    ports,
                    Box::pin(start(node)),
                ));
            }));
        handle
    }

    pub fn run(self) {
        let args = std::env::args().collect::<Vec<String>>();
        match args.iter().position(|arg| arg == "--node_id") {
            Some(index) => {
                let node_id = args[index + 1].parse::<usize>().unwrap();
                let coordinator_index = args
                    .iter()
                    .position(|arg| arg == "--coordinator_endpoint")
                    .unwrap();
                let coordinator_endpoint = args[coordinator_index + 1].clone();
                let runner = self.nodes.into_iter().nth(node_id).unwrap();
                runner(Rc::new(Runtime::new()), self.edges, coordinator_endpoint);
            }
            None => {
                let node_count = self.nodes.len();
                let exe = std::env::current_exe().unwrap();
                Runtime::new().block_on(async move {
                    let coordinator = Coordinator::<TcpTransport, _>::new(SerdeSerializer).await;
                    let endpoint = coordinator.endpoint().to_string();
                    let children = (0..node_count)
                        .map(|node_id| {
                            std::process::Command::new(&exe)
                                .arg("--node_id")
                                .arg(node_id.to_string())
                                .arg("--coordinator_endpoint")
                                .arg(&endpoint)
                                .args(&args[1..])
                                .spawn()
                                .unwrap()
                        })
                        .collect::<Vec<_>>();
                    coordinator.run(node_count).await;
                    for mut child in children {
                        let status = child.wait().unwrap();
                        assert!(status.success(), "node exited with {status} status");
                    }
                });
            }
        }
    }
}

async fn run_node(
    context: Rc<NodeContext>,
    node_id: NodeId,
    coordinator_endpoint: String,
    edges: Vec<Edge>,
    ports: HashMap<PortId, Rc<dyn Any>>,
    start: BoxFuture<()>,
) {
    let mut in_edges = Vec::new();
    let mut out_edges = Vec::new();
    for edge in edges {
        let Edge {
            from,
            to,
            serialize,
            deliver,
            close,
            install_sender,
            listener_factory,
            sender_factory,
        } = edge;
        if to.node_id == node_id {
            in_edges.push((to, deliver, close, listener_factory));
        }
        if from.node_id == node_id {
            out_edges.push((from, to, serialize, install_sender, sender_factory));
        }
    }

    let mut endpoints = HashMap::new();
    let mut receivers = Vec::new();
    for (to, deliver, close, listener_factory) in in_edges {
        let (endpoint, receiver_factory) = listener_factory().await;
        endpoints.insert(to, endpoint);
        receivers.push((to, deliver, close, receiver_factory));
    }

    let table =
        register::<TcpTransport, _>(&SerdeSerializer, &coordinator_endpoint, node_id, endpoints)
            .await;

    let mut send_futures = Vec::new();
    for (from, to, serialize, install_sender, sender_factory) in out_edges {
        let (frames, drain) = unbounded::<Vec<u8>>();
        let channel = ports
            .get(&from)
            .unwrap_or_else(|| panic!("port {from:?} was never built by its node's constructor"))
            .clone();
        let sender_channel = channel.clone();
        install_sender(
            &channel,
            Box::new(move |completed, promise| {
                let message = TaggedMessage {
                    timestamp: completed,
                    value: serialize(&sender_channel),
                    ent: promise,
                };
                frames
                    .unbounded_send(SerdeSerializer.serialize(&message))
                    .unwrap();
            }),
        );
        let endpoint = table.get(&to).unwrap().clone();
        send_futures.push(sender_factory(endpoint, drain));
    }

    let mut receive_futures = Vec::new();
    for (to, deliver, close, receiver_factory) in receivers {
        let channel = ports
            .get(&to)
            .unwrap_or_else(|| panic!("port {to:?} was never built by its node's constructor"))
            .clone();
        let channel_at_eof = channel.clone();
        let deliver_context = context.clone();
        let eof_context = context.clone();
        receive_futures.push(async move {
            receiver_factory(Box::new(move |frame| {
                let message: TaggedMessage = SerdeSerializer.deserialize(&frame);
                deliver(&channel, message);
                deliver_context.try_advance();
            }))
            .await;
            close(&channel_at_eof);
            eof_context.try_advance();
        });
    }

    let shutdown_context = context.clone();
    let main = async move {
        start.await;
        shutdown_context.send_events(shutdown_context.now(), Time::FOREVER);
        shutdown_context.close_event_drains();
    };

    futures::join!(main, join_all(send_futures), join_all(receive_futures));
}
