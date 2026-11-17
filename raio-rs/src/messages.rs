use serde::{Deserialize, Serialize};

use crate::{app::PortId, time::Time};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterNode {
    pub node_id: usize,
    pub endpoints: HashMap<PortId, String>,
}

pub type EndpointTable = HashMap<PortId, String>;

#[derive(Serialize, Deserialize, Debug)]
pub struct TaggedMessage {
    pub timestamp: Time,
    pub ent: Time,
    /// Physical messsages are re-stamped by the receiver.
    pub physical: bool,
    pub value: Option<Vec<u8>>,
}
