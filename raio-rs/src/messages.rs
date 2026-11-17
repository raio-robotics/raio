use serde::{Deserialize, Serialize};

use crate::time::Time;
use std::collections::HashMap;

/// A node's first message to its coordinator: it asks for the code its launcher left for it.
#[derive(Serialize, Deserialize, Debug)]
pub struct RequestCode {
    pub node_id: usize,
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

/// A node's wiring, or the reason the coordinator refused the graph.
pub type Reply = Result<Wiring, String>;

/// What a node sends its coordinator after it has registered.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub enum NodeMessage {
    /// Ask the coordinator to shut down every node at one tag, this long after their current tags.
    RequestShutdown(Time),
    /// The tag at which the node offers to shut down, in reply to `RequestNodeShutdown`.
    ShutdownTime(Time),
}

/// What the coordinator sends a node after its wiring.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub enum CoordinatorMessage {
    /// Ask the node to offer its current tag plus this delay as its shutdown tag.
    RequestNodeShutdown(Time),
    /// Shut down at this tag, the latest that any node offered.
    Shutdown(Time),
}

/// A connection's output for one tag, including its ENT promise and optional serialized value.
///
/// The sender adds the connection's delay, so both times are the receiver's.
#[derive(Serialize, Deserialize, Debug)]
pub struct Frame {
    pub timestamp: Time,
    pub ent: Time,
    /// Physical frames may be retagged to keep them ahead of the receiver's logical clock.
    pub physical: bool,
    pub value: Option<Vec<u8>>,
}
