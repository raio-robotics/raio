pub use crate::action::{Action, ActionHandle};
use crate::coordinator::{Coordinator, register};
use crate::messages::TaggedMessage;
pub use crate::node::Node;
use crate::node::{NodeContext, NodeId};
pub use crate::port::{Builder, InputPort, OutputPort, PortId};
use crate::port::{InputChannel, OutputChannel};
use crate::runtime::{BoxFuture, Runtime};
use crate::serializer::Serializer;
use crate::serializer::serde::SerdeSerializer;
use crate::time::Time;
use crate::transport::tcp::TcpTransport;
use crate::transport::{Receiver, Sender, Transport};
use futures::StreamExt;
use futures::channel::mpsc::unbounded;
use futures::future::join_all;
use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::time::Instant;

type Runner = Box<dyn FnOnce(Rc<Runtime>, Vec<Box<dyn Edge>>, String)>;
pub type Start<N> = Box<dyn FnOnce(Rc<N>) -> Pin<Box<dyn Future<Output = ()>>>>;

/// A connection with its types erased: `App` keeps every edge alike, and the node
/// process asks each one for its receiver or its sender.
pub(crate) trait Edge {
    fn from(&self) -> PortId;
    fn to(&self) -> PortId;
    /// Listen for the upstream node. Returns the endpoint to register and the task that
    /// delivers the upstream's frames to the input `port` until the upstream closes.
    fn listen(
        &self,
        port: Rc<dyn Any>,
        context: Rc<NodeContext>,
    ) -> BoxFuture<(String, BoxFuture<()>)>;
    /// Connect to the downstream node at `endpoint`. Installs the sender on the output
    /// `port` and returns the task that sends its frames until the port closes.
    fn connect_to(&self, endpoint: String, port: Rc<dyn Any>) -> BoxFuture<()>;
}

struct TypedEdge<Tr, T, S> {
    from: PortId,
    to: PortId,
    serializer: Rc<S>,
    delay: Time,
    types: PhantomData<fn() -> (Tr, T)>,
}

/// Queue a received frame on the input channel of an edge with `delay`.
fn deliver<T, S: Serializer<T>>(
    serializer: &S,
    delay: Time,
    channel: &InputChannel<T>,
    context: &NodeContext,
    message: TaggedMessage,
) {
    if let Some(bytes) = message.value {
        let value = serializer.deserialize(&bytes);
        if message.physical {
            // A physical value keeps the sender's tag plus the delay, moved only out of
            // this node's past: the edge delivers in the order it was sent.
            let tag = (message.timestamp + delay).max(context.now().next_microstep());
            channel.push_physical(tag, value);
        } else {
            channel.push(message.timestamp + delay, value);
        }
    }
    channel.update_remote_ent(message.ent + delay);
}

impl<Tr: Transport + 'static, T: 'static, S: Serializer<T> + 'static> Edge for TypedEdge<Tr, T, S> {
    fn from(&self) -> PortId {
        self.from
    }

    fn to(&self) -> PortId {
        self.to
    }

    fn listen(
        &self,
        port: Rc<dyn Any>,
        context: Rc<NodeContext>,
    ) -> BoxFuture<(String, BoxFuture<()>)> {
        let channel = port.downcast::<InputChannel<T>>().unwrap();
        let (serializer, delay) = (self.serializer.clone(), self.delay);
        Box::pin(async move {
            let mut transport = Tr::new().await;
            let endpoint = transport.endpoint().to_string();
            let receive: BoxFuture<()> = Box::pin(async move {
                let (_sender, mut receiver) = transport.accept().await;
                while let Some(frame) = receiver.recv().await {
                    let message: TaggedMessage = SerdeSerializer.deserialize(&frame);
                    deliver(&*serializer, delay, &channel, &context, message);
                }
                channel.close();
            });
            (endpoint, receive)
        })
    }

    fn connect_to(&self, endpoint: String, port: Rc<dyn Any>) -> BoxFuture<()> {
        let channel = port.downcast::<OutputChannel<T>>().unwrap();
        let serializer = self.serializer.clone();
        let (frames, mut queued) = unbounded::<Vec<u8>>();
        let value_channel = channel.clone();
        channel
            .senders
            .borrow_mut()
            .push(Box::new(move |timestamp, ent, physical| {
                let value = value_channel.cell.borrow_mut().take();
                let message = TaggedMessage {
                    timestamp,
                    ent,
                    physical,
                    value: value.map(|value| serializer.serialize(&value)),
                };
                frames
                    .unbounded_send(SerdeSerializer.serialize(&message))
                    .unwrap();
            }));
        Box::pin(async move {
            let (mut sender, _receiver) = Tr::connect(&endpoint).await;
            while let Some(frame) = queued.next().await {
                sender.send(&frame).await;
            }
        })
    }
}

pub struct App {
    edges: Vec<Box<dyn Edge>>,
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
            self.edges.iter().all(|edge| edge.to() != to.id),
            "input port {:?} already has an upstream; an input accepts exactly one connection",
            to.id
        );
        self.edges.push(Box::new(TypedEdge::<Tr, T, S> {
            from: from.id,
            to: to.id,
            serializer: Rc::new(serializer),
            delay,
            types: PhantomData,
        }));
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
        let ports = builder.next_port_id;
        self.nodes
            .push(Box::new(move |runtime, edges, coordinator_endpoint| {
                let node_context = Rc::new(NodeContext::new(runtime.clone()));
                let mut builder = Builder::new(node_id, node_context.clone());
                let node = Rc::new(constructor(&mut builder));
                assert!(
                    builder.next_port_id == ports,
                    "node {node_id}: constructor created a different number of ports \
                 than on its first run; constructors must be deterministic"
                );
                let ports = std::mem::take(&mut builder.ports);
                let inputs = std::mem::take(&mut builder.inputs);
                for task in std::mem::take(&mut builder.tasks) {
                    runtime.spawn_internal(task);
                }
                let frame_transfer = runtime.block_on(connect(
                    node_context.clone(),
                    node_id,
                    coordinator_endpoint,
                    edges,
                    ports,
                    inputs,
                ));
                let tasks = starts
                    .into_iter()
                    .map(|start| start(node.clone()))
                    .collect();
                node_context.run(frame_transfer, tasks);
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

/// Connect the node: listen for its inputs, register with the coordinator, connect to
/// its outputs. Returns the task that then sends and receives frames until every edge
/// has closed.
async fn connect(
    context: Rc<NodeContext>,
    node_id: NodeId,
    coordinator_endpoint: String,
    edges: Vec<Box<dyn Edge>>,
    ports: HashMap<PortId, Rc<dyn Any>>,
    inputs: Vec<PortId>,
) -> BoxFuture<()> {
    for input in &inputs {
        // An input without an upstream would promise nothing beyond tag 0 and hold the
        // node there forever.
        assert!(
            edges.iter().any(|edge| edge.to() == *input),
            "input port {input:?} has no upstream; connect it with App::connect"
        );
    }
    let port = |id: PortId| {
        ports
            .get(&id)
            .unwrap_or_else(|| panic!("port {id:?} was never built by its node's constructor"))
            .clone()
    };

    let mut endpoints = HashMap::new();
    let mut receives = Vec::new();
    for edge in edges.iter().filter(|edge| edge.to().node_id == node_id) {
        let (endpoint, receive) = edge.listen(port(edge.to()), context.clone()).await;
        endpoints.insert(edge.to(), endpoint);
        receives.push(receive);
    }

    let table =
        register::<TcpTransport, _>(&SerdeSerializer, &coordinator_endpoint, node_id, endpoints)
            .await;
    context.start_time.set(Instant::now());

    let sends: Vec<_> = edges
        .iter()
        .filter(|edge| edge.from().node_id == node_id)
        .map(|edge| edge.connect_to(table[&edge.to()].clone(), port(edge.from())))
        .collect();
    Box::pin(async move {
        futures::join!(join_all(sends), join_all(receives));
    })
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
    use crate::time::{MS, S};

    /// A node whose clock stands at 5s, receiving over a 10ms edge.
    fn node_at_5s() -> (Rc<NodeContext>, InputChannel<i64>) {
        let ctx = Rc::new(NodeContext::new(Rc::new(Runtime::new())));
        ctx.advance_to(5 * S);
        (ctx, InputChannel::new())
    }

    fn receive(
        ctx: &NodeContext,
        channel: &InputChannel<i64>,
        timestamp: Time,
        ent: Time,
        physical: bool,
    ) {
        let message = TaggedMessage {
            timestamp,
            ent,
            physical,
            value: Some(SerdeSerializer.serialize(&7i64)),
        };
        deliver(&SerdeSerializer, 10 * MS, channel, ctx, message);
    }

    #[test]
    fn a_logical_frame_keeps_its_tag_plus_the_delay() {
        let (ctx, channel) = node_at_5s();
        receive(&ctx, &channel, 7 * S, 6 * S, false);
        assert_eq!(channel.ready(), Some(7 * S + 10 * MS));
        assert_eq!(channel.ent(), 6 * S + 10 * MS);
    }

    #[test]
    fn a_physical_frame_from_this_nodes_past_is_moved_right_after_now() {
        let (ctx, channel) = node_at_5s();
        receive(&ctx, &channel, 1 * S, 6 * S, true); // 1s + 10ms is in the past
        assert_eq!(channel.ready(), Some((5 * S).next_microstep()));
        assert_eq!(channel.ent(), 6 * S + 10 * MS); // the promise is unaffected
    }

    #[test]
    fn a_physical_frame_keeps_the_senders_tag_plus_the_delay() {
        let (ctx, channel) = node_at_5s();
        receive(&ctx, &channel, 7 * S, 20 * S, true);
        assert_eq!(channel.ready(), Some(7 * S + 10 * MS));
    }

    #[test]
    fn an_edge_delivers_in_the_order_it_was_sent() {
        let (ctx, channel) = node_at_5s();
        receive(&ctx, &channel, 1 * S, 6 * S, true); // past: moved to (5s, 1)
        receive(&ctx, &channel, 2 * S, 6 * S, true); // past: moved behind it, (5s, 2)
        receive(&ctx, &channel, 6 * S, 20 * S, false); // 6s + 10ms, after both
        let after_now = (5 * S).next_microstep();
        for expected in [after_now, after_now.next_microstep(), 6 * S + 10 * MS] {
            assert_eq!(channel.ready(), Some(expected));
            channel.wake_wakers(expected);
        }
        assert_eq!(channel.ready(), None);
    }

    #[test]
    #[should_panic(expected = "has no upstream")]
    fn an_unconnected_input_is_a_wiring_bug() {
        let runtime = Rc::new(Runtime::new());
        let ctx = Rc::new(NodeContext::new(runtime.clone()));
        let mut builder = Builder::new(0, ctx.clone());
        let _input: InputPort<i64> = builder.input();
        let inputs = std::mem::take(&mut builder.inputs);
        let _frame_transfer = runtime.block_on(connect(
            ctx,
            0,
            "unused".into(),
            Vec::new(),
            HashMap::new(),
            inputs,
        ));
    }
}
