use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::runtime::Runtime;
use crate::time::Time;

pub type NodeId = usize;

pub trait EventSource {
    /// The earliest pending tag of this source.
    fn ent(&self) -> Time;
}

pub struct NodeContext {
    runtime: Rc<Runtime>,
    upstream_delays: HashMap<NodeId, Time>,
    ents: RefCell<HashMap<NodeId, Time>>,
    sources: RefCell<Vec<Rc<dyn EventSource>>>,
}

impl NodeContext {
    pub fn new(runtime: Rc<Runtime>, upstream_delays: HashMap<NodeId, Time>) -> Self {
        Self {
            runtime,
            upstream_delays,
            ents: RefCell::new(HashMap::new()),
            sources: RefCell::new(Vec::new()),
        }
    }

    pub fn now(&self) -> Time {
        self.runtime.now()
    }

    pub fn register_source(&self, source: Rc<dyn EventSource>) {
        self.sources.borrow_mut().push(source);
    }

    /// Safe To Advance To: The node may safely advance to any time t < stat().
    pub fn stat(&self) -> Time {
        let ents = self.ents.borrow();
        self.upstream_delays
            .iter()
            .map(|(node_id, delay)| ents.get(node_id).copied().unwrap_or(Time::ZERO) + *delay)
            .min()
            .unwrap_or(Time::FOREVER)
    }

    /// Earliest Next Timestamp: the smallest timestamp that this node will produce.
    pub fn ent(&self) -> Time {
        self.sources
            .borrow()
            .iter()
            .map(|source| source.ent())
            .min()
            .unwrap_or(Time::FOREVER)
            .max(self.now())
    }
}
