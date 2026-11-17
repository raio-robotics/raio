//! The scheduler of one node. It moves the node to the next timestamp that is safe to process
//! (`timestamps`), runs the tasks that the timestamp's events wake (`run`), and takes part in
//! the app's shutdown (`shutdown`).

mod event;
mod run;
mod shutdown;
#[cfg(test)]
mod testing;
mod timestamps;

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;
use std::time::Instant;

use super::ExecutionMode;
use super::alarms::Alarms;
use super::foreign::ForeignSource;
use crate::runtime::{Runtime, TaskId};
use crate::time::Time;
pub use event::{EventSink, EventSource};
use shutdown::{Request, Stage};

/// Owns the node's clock, event sources, outputs, and task scheduling decisions.
pub struct NodeScheduler {
    pub(crate) runtime: Rc<Runtime>,
    now: Cell<Time>,
    start_time: Cell<Instant>,
    event_sources: RefCell<Vec<Rc<dyn EventSource>>>,
    event_sinks: RefCell<Vec<Rc<dyn EventSink>>>,
    /// Prevents further output changes after the current timestamp's frame is sent.
    timestamp_complete: Cell<bool>,
    /// Records whether this timestamp includes a physical event.
    physical: Cell<bool>,
    foreign: Rc<ForeignSource>,
    pub(super) alarms: Rc<Alarms>,
    pub(crate) execution_mode: Cell<ExecutionMode>,
    /// Outputs close after all user tasks finish.
    user_tasks: RefCell<Vec<TaskId>>,
    /// Ends the run loop without completing the current timestamp.
    pub(super) aborted: Cell<bool>,
    shutdown: Cell<Stage>,
    /// Runs once when the node shuts down, with the app or on its own.
    pub(crate) shutdown_function: RefCell<Option<Box<dyn FnOnce()>>>,
    /// Carries the node's shutdown request and its delay to the coordinator.
    pub(crate) shutdown_request: RefCell<Option<Request>>,
}

impl NodeScheduler {
    pub fn new(runtime: Rc<Runtime>) -> Self {
        let foreign = Rc::new(ForeignSource::new());
        let alarms = Rc::new(Alarms::new());
        Self {
            runtime,
            now: Cell::new(Time::ZERO),
            start_time: Cell::new(Instant::now()),
            event_sources: RefCell::new(vec![foreign.clone(), alarms.clone()]),
            event_sinks: RefCell::new(Vec::new()),
            timestamp_complete: Cell::new(true),
            physical: Cell::new(false),
            foreign,
            alarms,
            execution_mode: Cell::new(ExecutionMode::default()),
            user_tasks: RefCell::new(Vec::new()),
            aborted: Cell::new(false),
            shutdown: Cell::new(Stage::Running),
            shutdown_function: RefCell::new(None),
            shutdown_request: RefCell::new(None),
        }
    }

    pub fn now(&self) -> Time {
        self.now.get()
    }

    pub fn physical_now(&self) -> Time {
        Time::new(self.start_time.get().elapsed().as_nanos() as i64)
    }

    /// True if the current timestamp was caused by a physical event.
    pub fn is_physical(&self) -> bool {
        self.physical.get()
    }

    /// True if the current timestamp's frame has been sent.
    pub fn timestamp_complete(&self) -> bool {
        self.timestamp_complete.get()
    }

    /// Choose a physical timestamp at wall-clock time or the next microstep, whichever is later.
    pub fn mint_physical_timestamp(&self) -> Time {
        self.physical_now().max(self.now().next_microstep())
    }

    pub(crate) fn add_source(&self, source: Rc<dyn EventSource>) {
        self.event_sources.borrow_mut().push(source);
    }

    /// Register an output sink to receive each completed timestamp's frame.
    pub(crate) fn add_sink(&self, sink: Rc<dyn EventSink>) {
        self.event_sinks.borrow_mut().push(sink);
    }

    /// Reset the wall-clock origin after node registration completes.
    pub(crate) fn start_clock(&self) {
        self.start_time.set(Instant::now());
    }

    /// Spawn a user task and track it for node shutdown.
    pub(crate) fn spawn(&self, task: impl Future<Output = ()> + 'static) -> TaskId {
        let task = self.runtime.spawn(task);
        self.user_tasks.borrow_mut().push(task);
        task
    }

    /// Remove finished user tasks and report whether none remain.
    fn user_tasks_finished(&self) -> bool {
        let mut tasks = self.user_tasks.borrow_mut();
        tasks.retain(|task| !self.runtime.finished(*task));
        tasks.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::{MS, S};

    #[test]
    fn a_physical_timestamp_behind_the_node_is_the_timestamp_right_after_now() {
        let scheduler = NodeScheduler::fast_forward();
        scheduler.advance_to(5 * S); // Logical time runs ahead of the wall clock.
        assert_eq!(
            scheduler.mint_physical_timestamp(),
            (5 * S).next_microstep()
        );
    }

    #[test]
    fn physical_now_runs_on_the_wall_clock() {
        let scheduler = NodeScheduler::fast_forward();
        scheduler.start_clock();
        let first = scheduler.physical_now();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let second = scheduler.physical_now();
        assert!(second >= first + 2 * MS, "{second} < {first} + 2ms");
        // Elapsed wall-clock time does not advance logical time.
        assert_eq!(scheduler.now(), Time::ZERO);
    }
}
