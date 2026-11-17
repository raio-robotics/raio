use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;

use futures::StreamExt;
use futures::channel::mpsc::unbounded;

use super::action::{Action, Handles};
use super::connection::{AnyInput, AnyOutput};
use super::port::{InputChannel, InputPort, OutputChannel, OutputPort};
use super::setting::PortSetting;
use crate::coordinator::PortId;
use crate::diagram::PortKind;
use crate::node::{ExecutionMode, Node, NodeId, NodeScheduler};
use crate::runtime::BoxFuture;
use crate::serializer::Payload;
use crate::time::Time;

thread_local! {
    /// The name that a port gets if it is added now: that of the field that `ports!` is at.
    static NAME: Cell<Option<&'static str>> = const { Cell::new(None) };
}

/// Name the ports that are added on this thread from now on; `ports!` calls this.
#[doc(hidden)]
pub fn name_ports(name: Option<&'static str>) {
    NAME.set(name);
}

/// Creates a node's ports, actions, and clock handles using one shared scheduler.
pub struct Builder {
    node_id: NodeId,
    scheduler: Rc<NodeScheduler>,
    /// The name and the kind of each port and action, in the order of their positions.
    pub(crate) ports: Vec<(String, PortKind)>,
    /// Each input port needs exactly one upstream connection.
    pub(crate) inputs: Vec<Rc<dyn AnyInput>>,
    pub(crate) outputs: Vec<Rc<dyn AnyOutput>>,
    /// Internal tasks that forward action values into input channels.
    pub(crate) internal_tasks: Vec<BoxFuture<()>>,
}

impl Builder {
    pub(crate) fn new(node_id: NodeId, scheduler: Rc<NodeScheduler>) -> Self {
        Self {
            node_id,
            scheduler,
            ports: Vec::new(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            internal_tasks: Vec::new(),
        }
    }

    /// Add an input port, which takes exactly one connection.
    pub fn input<T: Payload>(&mut self) -> InputPort<T> {
        let channel = Rc::new(InputChannel::new());
        let setting = Rc::new(PortSetting::new(self.next_port_id(PortKind::Input)));
        self.scheduler.add_source(channel.clone());
        self.inputs
            .push(Rc::new((channel.clone(), setting.clone())));
        InputPort {
            channel,
            setting,
            scheduler: self.scheduler.clone(),
        }
    }

    /// Add an action, which the threads of this program feed.
    pub fn action<T: Send + 'static>(&mut self) -> Action<T> {
        // An action takes a position among the node's ports, as it does where Python declares it.
        self.next_port_id(PortKind::Action);
        let channel = Rc::new(InputChannel::new());
        // Action values are physical, so they do not limit ENT.
        channel.update_remote_ent(Time::FOREVER);
        self.scheduler.add_source(channel.clone());
        let (sender, mut receiver) = unbounded::<T>();
        let (action_channel, scheduler) = (channel.clone(), self.scheduler.clone());
        self.internal_tasks.push(Box::pin(async move {
            while let Some(value) = receiver.next().await {
                action_channel.push_physical(scheduler.mint_physical_timestamp(), value);
            }
            action_channel.close(); // Dropping the last producer handle closes the action.
        }));
        Action {
            channel,
            scheduler: self.scheduler.clone(),
            handles: RefCell::new(Handles::Unused(Arc::new(sender))),
        }
    }

    /// Select `Realtime` (the default) or `FastForward` pacing for this node.
    pub fn execution_mode(&mut self, execution_mode: ExecutionMode) {
        self.scheduler.execution_mode.set(execution_mode);
    }

    pub fn node(&mut self) -> Node {
        Node {
            scheduler: self.scheduler.clone(),
        }
    }

    /// Add an output port, which may feed any number of inputs.
    pub fn output<T: Payload>(&mut self) -> OutputPort<T> {
        let setting = PortSetting::new(self.next_port_id(PortKind::Output));
        let channel = Rc::new(OutputChannel::new(setting));
        self.scheduler.add_sink(channel.clone());
        self.outputs.push(channel.clone());
        OutputPort {
            channel,
            scheduler: self.scheduler.clone(),
        }
    }

    fn next_port_id(&mut self, kind: PortKind) -> PortId {
        let port_id = self.ports.len();
        // A port that was given no name is known by its position.
        let name = NAME.get().map_or(port_id.to_string(), str::to_string);
        self.ports.push((name, kind));
        PortId {
            node_id: self.node_id,
            port_id,
        }
    }
}

/// Build a node and name each of its ports after the field that holds it. A diagram shows
/// these names.
#[macro_export]
macro_rules! ports {
    ($node:ident { $($field:ident: $value:expr),+ $(,)? }) => {{
        let node = $node {
            $($field: {
                $crate::app::name_ports(Some(stringify!($field)));
                $value
            }),+
        };
        $crate::app::name_ports(None);
        node
    }};
}
