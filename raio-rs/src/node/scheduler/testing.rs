use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;

use super::{EventSink, EventSource, NodeScheduler};
use crate::node::ExecutionMode;
use crate::runtime::Runtime;
use crate::time::Time;

pub(super) struct FixedSource {
    pub(super) ent: Cell<Time>,           // The source's logical ENT promise.
    pub(super) ready: Cell<Option<Time>>, // The next event available for delivery.
    physical: bool,
}
impl FixedSource {
    pub(super) fn new(ent: Time, ready: bool) -> Rc<Self> {
        Rc::new(Self {
            ent: Cell::new(ent),
            ready: Cell::new(ready.then_some(ent)),
            physical: false,
        })
    }
    /// Create a physical event that is excluded from ENT promises.
    pub(super) fn physical(tag: Time) -> Rc<Self> {
        Rc::new(Self {
            ent: Cell::new(Time::FOREVER),
            ready: Cell::new(Some(tag)),
            physical: true,
        })
    }
}
impl EventSource for FixedSource {
    fn ent(&self) -> Time {
        self.ent.get()
    }
    fn next_event_tag(&self) -> Option<Time> {
        self.ready.get()
    }
    fn wake_wakers(&self, _time: Time) -> bool {
        self.physical // This test source only reports the event's physical flag.
    }
}

/// The tag, ENT promise and physical flag of each frame a sink received.
pub(super) type Frames = Vec<(Time, Time, bool)>;

pub(super) struct RecordingSink {
    pub(super) sent: RefCell<Frames>,
    pub(super) closed: Cell<bool>,
}
impl RecordingSink {
    pub(super) fn new() -> Rc<Self> {
        Rc::new(Self {
            sent: RefCell::new(Vec::new()),
            closed: Cell::new(false),
        })
    }
}
impl EventSink for RecordingSink {
    fn send(&self, timestamp: Time, ent: Time, physical: bool) {
        self.sent.borrow_mut().push((timestamp, ent, physical));
    }
    fn close(&self) {
        self.closed.set(true);
    }
}

/// Create a FastForward node with a sink that records its output frames.
pub(super) fn node_rig() -> (Rc<Runtime>, Rc<NodeScheduler>, Rc<RecordingSink>) {
    let runtime = Rc::new(Runtime::new());
    let scheduler = Rc::new(NodeScheduler::with_execution_mode(
        runtime.clone(),
        ExecutionMode::FastForward,
    ));
    let sink = RecordingSink::new();
    scheduler.add_sink(sink.clone());
    scheduler.start();
    (runtime, scheduler, sink)
}

impl NodeScheduler {
    /// Complete this tag, then advance if event ordering and wall-clock pacing allow it.
    pub(crate) fn try_advance(&self) -> bool {
        self.send_frame(|| self.ent());
        self.advance_if_due(self.next_safe_tag())
    }

    /// Run one user task with no frame-transfer work.
    pub(crate) fn run_user(&self, task: impl Future<Output = ()> + 'static) {
        self.run(async {}, vec![Box::pin(task)]);
    }

    /// Run until no task is runnable and no tag can advance immediately.
    pub(crate) fn run_until_idle(&self) {
        self.run_until(|| true);
    }
}
