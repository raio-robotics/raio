use serde::{Deserialize, Serialize};

use crate::time::Time;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterNode {
    pub node_id: usize,
    pub ports: Vec<PortInfo>,
}

/// A port as its node describes it to the coordinator.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PortInfo {
    /// The port's position among the ports its node built.
    pub id: usize,
    pub serializers: Vec<String>,
    pub transports: Vec<String>,
    /// The address an input listens on with each of its transports; `None` for an output.
    pub endpoints: Option<HashMap<String, String>>,
}

/// The connection that feeds an input.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct Upstream {
    pub delay: Time,
    pub serializer: String,
    pub transport: String,
}

/// One connection that an output feeds.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct Downstream {
    pub endpoint: String,
    pub serializer: String,
    pub transport: String,
}

/// The connections of one node's ports, keyed by port id.
#[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug)]
pub struct Wiring {
    pub upstream: HashMap<usize, Upstream>,
    pub downstream: HashMap<usize, Vec<Downstream>>,
}

/// A node's wiring, or the reason the coordinator refused the graph.
pub type Reply = Result<Wiring, String>;

/// An edge's output for one tag, including its ENT promise and optional serialized value.
#[derive(Serialize, Deserialize, Debug)]
pub struct Frame {
    pub timestamp: Time,
    pub ent: Time,
    /// Physical frames may be retagged to keep them ahead of the receiver's logical clock.
    pub physical: bool,
    pub value: Option<Vec<u8>>,
}
