use serde::{Deserialize, Serialize};

use crate::time::Time;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterNode {
    pub node_id: usize,
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
