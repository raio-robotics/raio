pub use crate::action::{Action, ActionHandle};
use crate::coordinator::{Coordinator, register};
use crate::messages::Frame;
pub use crate::node::{ExecutionMode, Node};
use crate::node::{NodeId, NodeScheduler};
pub use crate::port::{Builder, InputPort, OutputPort, PortId};
use crate::port::{InputChannel, OutputChannel};
use crate::runtime::{BoxFuture, Runtime};
use crate::serializer::Serializer;
use crate::serializer::bincode::BincodeSerializer;
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

type Runner = Box<dyn FnOnce(Rc<Runtime>, Vec<Box<dyn Edge>>, String)>;
pub type Start<N> = Box<dyn FnOnce(Rc<N>) -> Pin<Box<dyn Future<Output = ()>>>>;

/// A port connection whose payload type is hidden from the app.
pub(crate) trait Edge {
    fn from(&self) -> PortId;
    fn to(&self) -> PortId;
    /// Create an input listener and return its address and frame-receiving task.
    fn listen(
        &self,
        port: Rc<dyn Any>,
        scheduler: Rc<NodeScheduler>,
    ) -> BoxFuture<(String, BoxFuture<()>)>;
    /// Attach an output sender and return the task that connects and sends its frames.
    fn connect_to(&self, endpoint: String, port: Rc<dyn Any>) -> BoxFuture<()>;
}

struct TypedEdge<Tr, T, S> {
    from: PortId,
    to: PortId,
    serializer: Rc<S>,
    delay: Time,
    types: PhantomData<fn() -> (Tr, T)>,
}

/// Apply the edge delay, queue the frame's value, and update the input's ENT.
fn deliver<T, S: Serializer<T>>(
    serializer: &S,
    delay: Time,
    channel: &InputChannel<T>,
    scheduler: &NodeScheduler,
    message: Frame,
) {
    if let Some(bytes) = message.value {
        let value = serializer.deserialize(&bytes);
        if message.physical {
            // Move late physical events to a tag after the node's current tag.
            let tag = (message.timestamp + delay).max(scheduler.now().next_microstep());
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
        scheduler: Rc<NodeScheduler>,
    ) -> BoxFuture<(String, BoxFuture<()>)> {
        let channel = port.downcast::<InputChannel<T>>().unwrap();
        let (serializer, delay) = (self.serializer.clone(), self.delay);
        Box::pin(async move {
            let mut transport = Tr::bind().await;
            let endpoint = transport.endpoint().to_string();
            let receive: BoxFuture<()> = Box::pin(async move {
                let (_sender, mut receiver) = transport.accept().await;
                while let Some(frame) = receiver.recv().await {
                    let message: Frame = BincodeSerializer.deserialize(&frame);
                    deliver(&*serializer, delay, &channel, &scheduler, message);
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
                let message = Frame {
                    timestamp,
                    ent,
                    physical,
                    value: value.map(|value| serializer.serialize(&value)),
                };
                frames
                    .unbounded_send(BincodeSerializer.serialize(&message))
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

/// Stores the node graph and launches one process per node.
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
        self.connect_with::<TcpTransport, T, BincodeSerializer>(
            from,
            to,
            BincodeSerializer,
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
        let scheduler = Rc::new(NodeScheduler::new(Rc::new(Runtime::new())));
        let mut builder = Builder::new(node_id, scheduler);
        // Run the constructor here for wiring and again in the node's child process.
        let handle = constructor(&mut builder);
        let ports = builder.next_port_id;
        self.nodes
            .push(Box::new(move |runtime, edges, coordinator_endpoint| {
                let scheduler = Rc::new(NodeScheduler::new(runtime.clone()));
                let mut builder = Builder::new(node_id, scheduler.clone());
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
                    scheduler.clone(),
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
                scheduler.run(frame_transfer, tasks);
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
                    let coordinator = Coordinator::<TcpTransport, _>::new(BincodeSerializer).await;
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

/// Register input listeners and return the task that transfers frames over all edges.
async fn connect(
    scheduler: Rc<NodeScheduler>,
    node_id: NodeId,
    coordinator_endpoint: String,
    edges: Vec<Box<dyn Edge>>,
    ports: HashMap<PortId, Rc<dyn Any>>,
    inputs: Vec<PortId>,
) -> BoxFuture<()> {
    for input in &inputs {
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
        let (endpoint, receive) = edge.listen(port(edge.to()), scheduler.clone()).await;
        endpoints.insert(edge.to(), endpoint);
        receives.push(receive);
    }

    let table = register::<TcpTransport, _>(
        &BincodeSerializer,
        &coordinator_endpoint,
        node_id,
        endpoints,
    )
    .await;
    scheduler.start();

    let sends: Vec<_> = edges
        .iter()
        .filter(|edge| edge.from().node_id == node_id)
        .map(|edge| edge.connect_to(table[&edge.to()].clone(), port(edge.from())))
        .collect();
    Box::pin(async move {
        futures::join!(join_all(sends), join_all(receives));
    })
}

/// Package async node methods as startup tasks for `App::create_node`.
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

    /// Create a node at logical time 5s for edge-delivery tests.
    fn node_at_5s() -> (Rc<NodeScheduler>, InputChannel<i64>) {
        let scheduler = Rc::new(NodeScheduler::new(Rc::new(Runtime::new())));
        scheduler.advance_to(5 * S);
        (scheduler, InputChannel::new())
    }

    fn receive(
        scheduler: &NodeScheduler,
        channel: &InputChannel<i64>,
        timestamp: Time,
        ent: Time,
        physical: bool,
    ) {
        let message = Frame {
            timestamp,
            ent,
            physical,
            value: Some(BincodeSerializer.serialize(&7i64)),
        };
        deliver(&BincodeSerializer, 10 * MS, channel, scheduler, message);
    }

    #[test]
    fn a_logical_frame_keeps_its_tag_plus_the_delay() {
        let (scheduler, channel) = node_at_5s();
        receive(&scheduler, &channel, 7 * S, 6 * S, false);
        assert_eq!(channel.next_event_tag(), Some(7 * S + 10 * MS));
        assert_eq!(channel.ent(), 6 * S + 10 * MS);
    }

    #[test]
    fn a_physical_frame_from_this_nodes_past_is_moved_right_after_now() {
        let (scheduler, channel) = node_at_5s();
        // The delayed sender tag is behind this node's clock.
        receive(&scheduler, &channel, 1 * S, 6 * S, true);
        assert_eq!(channel.next_event_tag(), Some((5 * S).next_microstep()));
        // Retagging a physical value does not change its ENT promise.
        assert_eq!(channel.ent(), 6 * S + 10 * MS);
    }

    #[test]
    fn a_physical_frame_keeps_the_senders_tag_plus_the_delay() {
        let (scheduler, channel) = node_at_5s();
        receive(&scheduler, &channel, 7 * S, 20 * S, true);
        assert_eq!(channel.next_event_tag(), Some(7 * S + 10 * MS));
    }

    #[test]
    fn an_edge_delivers_in_the_order_it_was_sent() {
        let (scheduler, channel) = node_at_5s();
        // A late physical value moves to (5s, 1).
        receive(&scheduler, &channel, 1 * S, 6 * S, true);
        // The next late value follows at (5s, 2).
        receive(&scheduler, &channel, 2 * S, 6 * S, true);
        // The logical value follows both physical values.
        receive(&scheduler, &channel, 6 * S, 20 * S, false);
        let after_now = (5 * S).next_microstep();
        for expected in [after_now, after_now.next_microstep(), 6 * S + 10 * MS] {
            assert_eq!(channel.next_event_tag(), Some(expected));
            channel.wake_wakers(expected);
        }
        assert_eq!(channel.next_event_tag(), None);
    }

    #[test]
    #[should_panic(expected = "has no upstream")]
    fn an_unconnected_input_is_a_wiring_bug() {
        let runtime = Rc::new(Runtime::new());
        let scheduler = Rc::new(NodeScheduler::new(runtime.clone()));
        let mut builder = Builder::new(0, scheduler.clone());
        let _input: InputPort<i64> = builder.input();
        let inputs = std::mem::take(&mut builder.inputs);
        let _frame_transfer = runtime.block_on(connect(
            scheduler,
            0,
            "unused".into(),
            Vec::new(),
            HashMap::new(),
            inputs,
        ));
    }
}
