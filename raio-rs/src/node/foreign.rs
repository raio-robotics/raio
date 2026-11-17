use std::cell::RefCell;
use std::collections::HashMap;
use std::task::Waker;

use super::EventSource;
use crate::runtime::TaskId;
use crate::time::Time;

/// Tasks waiting for their assigned physical resumption timestamps.
pub(super) struct ForeignSource {
    pending: RefCell<HashMap<TaskId, (Time, Waker)>>,
}

impl ForeignSource {
    pub(super) fn new() -> Self {
        Self {
            pending: RefCell::new(HashMap::new()),
        }
    }

    /// Schedule a task's resumption once; repeated wakes keep its original timestamp.
    pub(super) fn defer(&self, task: TaskId, resumption: impl FnOnce() -> (Time, Waker)) {
        self.pending
            .borrow_mut()
            .entry(task)
            .or_insert_with(resumption);
    }

    /// Cancel a task's deferred resumption; return whether one existed.
    pub(super) fn release(&self, task: TaskId) -> bool {
        self.pending.borrow_mut().remove(&task).is_some()
    }
}

impl EventSource for ForeignSource {
    fn ent(&self) -> Time {
        Time::FOREVER // Foreign completions do not limit the logical ENT.
    }

    fn next_event_timestamp(&self) -> Option<Time> {
        self.pending
            .borrow()
            .values()
            .map(|(timestamp, _)| *timestamp)
            .min()
    }

    fn wake_wakers(&self, time: Time) -> bool {
        let due: Vec<_> = self
            .pending
            .borrow_mut()
            .extract_if(|_, (timestamp, _)| *timestamp <= time)
            .collect();
        for (_, (_, resume)) in &due {
            resume.wake_by_ref();
        }
        !due.is_empty()
    }
}
