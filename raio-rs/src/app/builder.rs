use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use futures::StreamExt;
use futures::channel::mpsc::unbounded;

use super::connection::{AnyInput, AnyOutput};
use super::port::{InputChannel, InputPort, OutputChannel, OutputPort, PortId};
use super::setting::PortSetting;
use crate::action::{Action, Handles};
use crate::node::{ExecutionMode, Node, NodeId, NodeScheduler};
use crate::runtime::BoxFuture;
use crate::serializer::Payload;
use crate::time::Time;

/// Creates a node's ports, actions, and clock handles using one shared scheduler.
pub struct Builder {
    node_id: NodeId,
    next_port_id: usize,
    scheduler: Rc<NodeScheduler>,
    /// Each input port needs exactly one upstream connection.
    pub(crate) inputs: Vec<Rc<dyn AnyInput>>,
    pub(crate) outputs: Vec<Rc<dyn AnyOutput>>,
    /// Internal tasks that forward action values into input channels.
    pub(crate) tasks: Vec<BoxFuture<()>>,
}

impl Builder {
    pub(crate) fn new(node_id: NodeId, scheduler: Rc<NodeScheduler>) -> Self {
        Self {
            node_id,
            next_port_id: 0,
            scheduler,
            inputs: Vec::new(),
            outputs: Vec::new(),
            tasks: Vec::new(),
        }
    }

    pub fn input<T: Payload>(&mut self) -> InputPort<T> {
        let setting = PortSetting::new(self.next_port_id());
        let channel = Rc::new(InputChannel::with_setting(setting));
        self.scheduler.add_source(channel.clone());
        self.inputs.push(channel.clone());
        InputPort {
            channel,
            scheduler: self.scheduler.clone(),
        }
    }

    pub fn action<T: Send + 'static>(&mut self) -> Action<T> {
        self.next_port_id(); // An action takes a position among the node's ports.
        let channel = Rc::new(InputChannel::new());
        // Action values are physical, so they do not limit ENT.
        channel.update_remote_ent(Time::FOREVER);
        self.scheduler.add_source(channel.clone());
        let (sender, mut receiver) = unbounded::<T>();
        let (action_channel, scheduler) = (channel.clone(), self.scheduler.clone());
        self.tasks.push(Box::pin(async move {
            while let Some(value) = receiver.next().await {
                action_channel.push_physical(scheduler.mint_physical_tag(), value);
            }
            action_channel.close(); // Dropping the last producer handle closes the action.
        }));
        Action {
            port: InputPort {
                channel,
                scheduler: self.scheduler.clone(),
            },
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

    pub fn output<T: Payload>(&mut self) -> OutputPort<T> {
        let setting = PortSetting::new(self.next_port_id());
        let channel = Rc::new(OutputChannel::new(setting));
        self.scheduler.add_sink(channel.clone());
        self.outputs.push(channel.clone());
        OutputPort {
            channel,
            scheduler: self.scheduler.clone(),
        }
    }

    fn next_port_id(&mut self) -> PortId {
        let id = PortId {
            node_id: self.node_id,
            port_id: self.next_port_id,
        };
        self.next_port_id += 1;
        id
    }
}
