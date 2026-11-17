use serde::{Deserialize, Serialize};

use crate::{app::PortId, time::Time};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterNode {
    pub node_id: usize,
    pub endpoints: HashMap<PortId, String>,
}

pub type EndpointTable = HashMap<PortId, String>;

/// An edge's output for one tag, including its ENT promise and optional serialized value.
#[derive(Serialize, Deserialize, Debug)]
pub struct Frame {
    pub timestamp: Time,
    pub ent: Time,
    /// Physical frames may be retagged to keep them ahead of the receiver's logical clock.
    pub physical: bool,
    pub value: Option<Vec<u8>>,
}
