use serde::{Deserialize, Serialize};

use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterNode {
    pub node_id: usize,
    pub endpoint: String,
}

pub type EndpointTable = HashMap<usize, String>;
