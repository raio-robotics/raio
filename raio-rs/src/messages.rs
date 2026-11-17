//! Everything that crosses a process boundary: the messages between a node and its coordinator,
//! in the order of their stages, and the frame that a connection carries.

use serde::{Deserialize, Serialize};

use crate::node::NodeId;
use crate::time::Time;
use std::collections::HashMap;

/// A node's first message to its coordinator: it asks for the code its launcher left for it.
#[derive(Serialize, Deserialize, Debug)]
pub struct RequestCode {
    pub node_id: NodeId,
}

/// The code the coordinator holds for a node; a node written in Rust has none.
pub type Code = Option<Vec<u8>>;

/// A node's ports, sent once the node has its code.
#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterNode {
    pub inputs: Vec<InputInfo>,
    pub outputs: Vec<OutputInfo>,
}

/// An input as its node describes it; an input uses one serializer and one transport.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct InputInfo {
    /// The port's position among the ports its node built.
    pub id: usize,
    pub serializer: String,
    pub transport: String,
    /// The address the input listens on.
    pub endpoint: String,
}

/// An output as its node describes it, with every serializer and transport it has.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct OutputInfo {
    /// The port's position among the ports its node built.
    pub id: usize,
    pub serializers: Vec<String>,
    pub transports: Vec<String>,
}

/// One connection that an output feeds: the input it reaches and the delay it adds.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct Downstream {
    pub input: InputInfo,
    pub delay: Time,
}

/// The connections that each output of one node feeds, keyed by port id.
pub type Wiring = HashMap<usize, Vec<Downstream>>;

/// The coordinator's reply to a node that has registered.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub enum Reply {
    /// The app starts, and this is the node's wiring.
    Wired(Wiring),
    /// The graph does not fit the ports that the nodes registered, for this reason.
    Refused(String),
    /// Another node failed before the app was wired, so the app does not start.
    Cancelled,
}

/// A message from a node to its coordinator, once the node has registered.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub enum NodeMessage {
    /// Ask the coordinator to shut down every node at one timestamp, this long after their
    /// current timestamps.
    RequestShutdown(Time),
    /// The timestamp at which the node offers to shut down, in reply to `RequestNodeShutdown`.
    ShutdownTime(Time),
    /// The node still runs; a running node sends this every `HEARTBEAT_PERIOD`.
    Heartbeat,
}

/// A message from the coordinator to a node, once the node is wired.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub enum CoordinatorMessage {
    /// Ask the node to offer its current timestamp plus this delay as its shutdown timestamp.
    RequestNodeShutdown(Time),
    /// Shut down at this timestamp, the latest that any node offered.
    Shutdown(Time),
    /// Shut down at once: the app was interrupted, or one of its nodes failed.
    ShutdownNow,
}

/// A connection's output for one timestamp, including its ENT promise and optional serialized
/// value.
///
/// The sender adds the connection's delay, so both times are the receiver's.
#[derive(Serialize, Deserialize, Debug)]
pub struct Frame {
    pub timestamp: Time,
    pub ent: Time,
    /// A physical frame may get a later timestamp, which keeps it ahead of the receiver's clock.
    pub physical: bool,
    pub value: Option<Vec<u8>>,
}
