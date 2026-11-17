pub use crate::action::{Action, ActionHandle};
use crate::coordinator::{Coordinator, register};
use crate::messages::TaggedMessage;
pub use crate::node::Node;
use crate::node::{NodeContext, NodeId};
pub use crate::port::{Builder, InputPort, OutputPort, PortId};
use crate::port::{InputChannel, OutputChannel, OutputSender};
use crate::runtime::Runtime;
use crate::serializer::Serializer;
use crate::serializer::serde::SerdeSerializer;
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
pub type Start<N> = Box<dyn FnOnce(Rc<N>) -> Pin<Box<dyn Future<Output = ()>>>>;
pub(crate) type BoxFuture<T> = Pin<Box<dyn Future<Output = T>>>;
pub(crate) type ReceiverFactory = Box<dyn FnOnce(Box<dyn Fn(Vec<u8>)>) -> BoxFuture<()>>;
pub(crate) type ListenerFactory = Box<dyn FnOnce() -> BoxFuture<(String, ReceiverFactory)>>;
pub(crate) type SenderFactory =
    Box<dyn FnOnce(String, UnboundedReceiver<Vec<u8>>) -> BoxFuture<()>>;

pub struct Edge {
    pub(crate) from: PortId,
    pub(crate) to: PortId,
    pub(crate) serialize: Box<dyn Fn(&Rc<dyn Any>) -> Option<Vec<u8>>>,
    pub(crate) deliver: Box<dyn Fn(&Rc<dyn Any>, &NodeContext, TaggedMessage)>,
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
        let deliver = Box::new(
            move |channel: &Rc<dyn Any>, context: &NodeContext, message: TaggedMessage| {
                let channel = channel.clone().downcast::<InputChannel<T>>().unwrap();
                if let Some(bytes) = message.value {
                    let value = serializer.deserialize(&bytes);
                    if message.physical {
                        // The sender's tag means nothing here: the value is retagged on arrival.
                        channel.push_physical(context.physical_tag(delay), value);
                    } else {
                        channel.push(message.timestamp + delay, value);
                    }
                }
                channel.update_remote_ent(message.ent + delay);
            },
        );
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

    pub fn create_node<N: 'static>(
        &mut self,
        constructor: impl Fn(&mut Builder) -> N + 'static,
        starts: Vec<Start<N>>,
    ) -> N {
        let node_id: NodeId = self.nodes.len() as NodeId;
        let node_context = Rc::new(NodeContext::new(Rc::new(Runtime::new())));
        let mut builder = Builder::new(node_id, node_context);
        let handle = constructor(&mut builder);
        let ports = builder.port_count();
        self.nodes
            .push(Box::new(move |runtime, edges, coordinator_endpoint| {
                let node_context = Rc::new(NodeContext::new(runtime.clone()));
                let mut builder = Builder::new(node_id, node_context.clone());
                let node = Rc::new(constructor(&mut builder));
                assert!(
                    builder.port_count() == ports,
                    "node {node_id}: constructor created a different number of ports \
                 than on its first run; constructors must be deterministic"
                );
                let ports = std::mem::take(&mut builder.ports);
                for task in std::mem::take(&mut builder.action_tasks) {
                    runtime.spawn(task);
                }
                let futures: Vec<_> = starts
                    .into_iter()
                    .map(|start| start(node.clone()))
                    .collect();
                let idle_context = node_context.clone();
                runtime.block_on_with_idle(
                    run_node(
                        node_context,
                        node_id,
                        coordinator_endpoint,
                        edges,
                        ports,
                        Box::pin(async move {
                            join_all(futures).await;
                        }),
                    ),
                    move || idle_context.try_advance(),
                );
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

    context.update_start_time();

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
            Box::new(move |completed, promise, physical| {
                let message = TaggedMessage {
                    timestamp: completed,
                    value: serialize(&sender_channel),
                    ent: promise,
                    physical,
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
        receive_futures.push(async move {
            receiver_factory(Box::new(move |frame| {
                let message: TaggedMessage = SerdeSerializer.deserialize(&frame);
                deliver(&channel, &deliver_context, message);
            }))
            .await;
            close(&channel_at_eof);
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

#[macro_export]
macro_rules! register {
    ($($start:expr),+ $(,)?) => {
        vec![$({
            let start: $crate::app::Start<_> =
                Box::new(move |node| Box::pin($start(node)));
            start
        }),+]
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::EventSource;
    use crate::time::{MS, NS, S};
    use std::time::{Duration, Instant};

    /// One 10ms edge into a node whose clock stands at 5s.
    fn edge_into_node_at_5s() -> (App, Rc<NodeContext>, Rc<InputChannel<i64>>) {
        let mut app = App::new();
        let ctx = Rc::new(NodeContext::new(Rc::new(Runtime::new())));
        let mut builder = Builder::new(0, ctx.clone());
        let out: OutputPort<i64> = builder.output();
        let input: InputPort<i64> = builder.input();
        app.connect_with::<TcpTransport, _, _>(&out, &input, SerdeSerializer, 10 * MS);
        ctx.advance_to(5 * S);
        (app, ctx, input.channel.clone())
    }

    fn frame(timestamp: Time, ent: Time, physical: bool) -> TaggedMessage {
        TaggedMessage {
            timestamp,
            ent,
            physical,
            value: Some(SerdeSerializer.serialize(&7i64)),
        }
    }

    #[test]
    fn a_logical_frame_keeps_its_tag_plus_the_delay() {
        let (app, ctx, channel) = edge_into_node_at_5s();
        let any = channel.clone() as Rc<dyn Any>;
        (app.edges[0].deliver)(&any, &ctx, frame(7 * S, 6 * S, false));
        assert_eq!(channel.ready(), Some(7 * S + 10 * MS));
        assert_eq!(channel.ent(), 6 * S + 10 * MS);
    }

    #[test]
    fn a_physical_frame_is_restamped_on_arrival() {
        let (app, ctx, channel) = edge_into_node_at_5s();
        let any = channel.clone() as Rc<dyn Any>;
        // The sender's tag (1s) and this node's wall clock (~0) are both in its past.
        (app.edges[0].deliver)(&any, &ctx, frame(1 * S, 6 * S, true));
        assert_eq!(channel.ready(), Some(5 * S + NS));
        assert_eq!(channel.ent(), 6 * S + 10 * MS); // the promise is unaffected
    }

    #[test]
    fn a_physical_frame_takes_the_wall_clock_when_it_is_later() {
        let (app, ctx, channel) = edge_into_node_at_5s();
        let any = channel.clone() as Rc<dyn Any>;
        ctx.start_time.set(Instant::now() - Duration::from_secs(9)); // 9s of wall time have passed
        // The promise (20s) is past the stamp, so the head is in hand.
        (app.edges[0].deliver)(&any, &ctx, frame(1 * S, 20 * S, true));
        let stamp = channel.ready().unwrap();
        assert!(stamp >= 9 * S + 10 * MS, "{stamp}");
        assert!(stamp < 10 * S, "{stamp}");
    }
}
