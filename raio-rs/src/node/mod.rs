mod alarms;
mod foreign;

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;
use std::task::Poll;
use std::time::Instant;

use crate::runtime::{BoxFuture, Runtime, TaskId, WakeKind};
use crate::time::Time;
use alarms::Alarms;
use foreign::ForeignSource;

pub type NodeId = usize;

/// Supplies events and ENT promises so the scheduler can order tags safely.
pub trait EventSource {
    /// Earliest Next Timestamp (ENT): no logical event may arrive at a smaller tag.
    fn ent(&self) -> Time;

    /// The next deliverable tag, or `None` if unknown; physical time may still lag behind.
    fn next_event_tag(&self) -> Option<Time>;

    /// Deliver events through `time`, wake their tasks, and report whether any event was physical.
    fn wake_wakers(&self, time: Time) -> bool;
}

/// Receives completed tag frames and closes when the node finishes producing output.
pub trait EventSink {
    fn send(&self, timestamp: Time, ent: Time, physical: bool);
    fn close(&self);
}

/// Provides user tasks with the node's clock and waiting operations.
pub struct Node {
    pub(crate) scheduler: Rc<NodeScheduler>,
}

impl Node {
    pub fn now(&self) -> Time {
        self.scheduler.now()
    }

    pub fn physical_now(&self) -> Time {
        self.scheduler.physical_now()
    }

    /// True if the current tag was caused by a physical event.
    pub fn is_physical(&self) -> bool {
        self.scheduler.is_physical()
    }

    /// True if the current tag's frame has been sent.
    pub fn tag_complete(&self) -> bool {
        self.scheduler.tag_complete()
    }

    /// Spawn another user task on this node.
    pub fn spawn(&self, task: impl Future<Output = ()> + 'static) {
        self.scheduler.spawn(task);
    }

    /// Stop running the node as soon as the current poll returns.
    pub fn abort(&self) {
        self.scheduler.aborted.set(true);
    }

    pub async fn wait(&self, duration: Time) -> Time {
        assert!(duration > Time::ZERO, "wait needs a positive duration");
        let target = self.scheduler.now() + duration;
        let alarm = self
            .scheduler
            .alarms
            .set(target, self.scheduler.is_physical());
        std::future::poll_fn(|cx| {
            if self.scheduler.now() >= target {
                return Poll::Ready(());
            }
            *alarm.waker.borrow_mut() = Some(Runtime::waker(cx, WakeKind::Resume));
            Poll::Pending
        })
        .await;
        self.scheduler.now()
    }

    /// Await a future and request physical resumption, including when it was already complete.
    pub async fn await_physical<F: Future>(&self, future: F) -> F::Output {
        let output = future.await;
        let mut deferred = false;
        std::future::poll_fn(|cx| {
            if deferred {
                return Poll::Ready(());
            }
            deferred = true;
            self.scheduler.defer(Runtime::current_task(cx));
            Poll::Pending
        })
        .await;
        output
    }
}

/// Controls whether logical execution waits for the wall clock.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExecutionMode {
    /// Execute each safe tag once physical time reaches it.
    #[default]
    Realtime,
    /// Execute safe tags without wall-clock delays; I/O and ENT promises still apply.
    FastForward,
}

/// Owns the node's clock, event sources, outputs, and task scheduling decisions.
pub struct NodeScheduler {
    runtime: Rc<Runtime>,
    now: Cell<Time>,
    start_time: Cell<Instant>,
    event_sources: RefCell<Vec<Rc<dyn EventSource>>>,
    event_sinks: RefCell<Vec<Rc<dyn EventSink>>>,
    /// Prevents further output changes after the current tag's frame is sent.
    tag_complete: Cell<bool>,
    /// Records whether this tag includes a physical event.
    physical: Cell<bool>,
    foreign: Rc<ForeignSource>,
    alarms: Rc<Alarms>,
    execution_mode: Cell<ExecutionMode>,
    /// Outputs close after all user tasks finish.
    user_tasks: RefCell<Vec<TaskId>>,
    /// Ends the run loop without completing the current tag.
    aborted: Cell<bool>,
}

impl NodeScheduler {
    /// Create a scheduler with the default execution mode, `Realtime`.
    pub fn new(runtime: Rc<Runtime>) -> Self {
        Self::with_execution_mode(runtime, ExecutionMode::default())
    }

    /// Create a scheduler with an explicit execution mode.
    pub fn with_execution_mode(runtime: Rc<Runtime>, execution_mode: ExecutionMode) -> Self {
        let foreign = Rc::new(ForeignSource::new());
        let alarms = Rc::new(Alarms::new());
        Self {
            runtime,
            now: Cell::new(Time::ZERO),
            start_time: Cell::new(Instant::now()),
            event_sources: RefCell::new(vec![foreign.clone(), alarms.clone()]),
            event_sinks: RefCell::new(Vec::new()),
            tag_complete: Cell::new(true),
            physical: Cell::new(false),
            foreign,
            alarms,
            execution_mode: Cell::new(execution_mode),
            user_tasks: RefCell::new(Vec::new()),
            aborted: Cell::new(false),
        }
    }

    pub fn now(&self) -> Time {
        self.now.get()
    }

    pub fn physical_now(&self) -> Time {
        Time::new(self.start_time.get().elapsed().as_nanos() as i64)
    }

    /// True if the current tag was caused by a physical event.
    pub fn is_physical(&self) -> bool {
        self.physical.get()
    }

    /// True if the current tag's frame has been sent.
    pub fn tag_complete(&self) -> bool {
        self.tag_complete.get()
    }

    /// Choose a physical tag at wall-clock time or the next microstep, whichever is later.
    pub fn mint_physical_tag(&self) -> Time {
        self.physical_now().max(self.now().next_microstep())
    }

    /// Register an event source with this node's scheduler.
    pub(crate) fn add_source(&self, source: Rc<dyn EventSource>) {
        self.event_sources.borrow_mut().push(source);
    }

    /// Register an output sink to receive each completed tag's frame.
    pub(crate) fn add_sink(&self, sink: Rc<dyn EventSink>) {
        self.event_sinks.borrow_mut().push(sink);
    }

    /// Set the execution mode while building the node.
    pub(crate) fn set_execution_mode(&self, execution_mode: ExecutionMode) {
        self.execution_mode.set(execution_mode);
    }

    /// Reset the wall-clock origin after node registration completes.
    pub(crate) fn start(&self) {
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

    /// Advance logical time and deliver all events through the new tag.
    pub fn advance_to(&self, time: Time) {
        assert!(
            time > self.now.get(),
            "cannot advance to {time} from {}",
            self.now.get()
        );
        self.now.set(time);
        self.tag_complete.set(false);
        let mut physical = false;
        for source in self.event_sources.borrow().iter() {
            physical |= source.wake_wakers(time);
        }
        self.physical.set(physical);
    }

    /// Send the current tag's output and ENT promise exactly once.
    fn send_frame(&self, ent: impl FnOnce() -> Time) {
        if self.tag_complete.get() {
            return;
        }
        let (timestamp, ent) = (self.now(), ent());
        assert!(
            ent > timestamp,
            "ENT would be less than timestamp: {ent} < {timestamp}"
        );
        for sink in self.event_sinks.borrow().iter() {
            sink.send(timestamp, ent, self.physical.get());
        }
        self.tag_complete.set(true);
    }

    /// Finish the current tag by sending its output and the node's ENT promise.
    pub fn complete_tag(&self) {
        self.send_frame(|| self.ent());
    }

    /// Send any unsent final frame with ENT = FOREVER, then close the output sinks.
    fn shut_down(&self) {
        self.send_frame(|| Time::FOREVER);
        for sink in self.event_sinks.borrow_mut().drain(..) {
            sink.close();
        }
    }

    /// Return the minimum ENT across all sources, excluding physical events.
    pub fn ent(&self) -> Time {
        self.event_sources
            .borrow()
            .iter()
            .map(|source| source.ent())
            .min()
            .unwrap_or(Time::FOREVER)
    }

    /// Return the earliest event tag that every source's ordering constraints permit.
    fn next_safe_tag(&self) -> Option<Time> {
        let sources = self.event_sources.borrow();
        let ready: Vec<Option<Time>> = sources
            .iter()
            .map(|source| source.next_event_tag())
            .collect();
        // Each reported tag rules out earlier arrivals from that source.
        let next = ready.iter().flatten().min().copied()?;
        let stat = sources
            .iter()
            .zip(&ready)
            .filter(|(_, ready)| ready.is_none())
            .map(|(source, _)| source.ent())
            .min()
            .unwrap_or(Time::FOREVER);
        // Equality must wait: a source with ENT = 2s may still deliver at 2s.
        (next > self.now() && next < stat).then_some(next)
    }

    /// Return when a tag is due on the wall clock, or `None` in FastForward mode.
    fn due(&self, tag: Time) -> Option<Instant> {
        match self.execution_mode.get() {
            ExecutionMode::Realtime => Some(self.start_time.get() + tag.duration()),
            ExecutionMode::FastForward => None,
        }
    }

    /// Check whether a tag is due on the wall clock or pacing is disabled.
    fn is_due(&self, tag: Time) -> bool {
        self.due(tag).is_none_or(|due| due <= Instant::now())
    }

    /// Advance if the next tag is due; return whether time advanced.
    fn advance_if_due(&self, next: Option<Time>) -> bool {
        match next {
            Some(tag) if self.is_due(tag) => {
                self.advance_to(tag);
                true
            }
            _ => false,
        }
    }

    /// Complete this tag, then advance if event ordering and wall-clock pacing allow it.
    pub fn try_advance(&self) -> bool {
        self.complete_tag();
        self.advance_if_due(self.next_safe_tag())
    }
}

impl NodeScheduler {
    /// Run until frame transfer and all user tasks finish.
    pub(crate) fn run(
        &self,
        frame_transfer: impl Future<Output = ()> + 'static,
        tasks: Vec<BoxFuture<()>>,
    ) {
        let root = self.runtime.spawn_internal(frame_transfer);
        for task in tasks {
            self.spawn(task);
        }
        self.run_until(|| self.runtime.finished(root) && self.user_tasks_finished());
    }

    /// Process task wakes and advance tags until idle and done.
    fn run_until(&self, done: impl Fn() -> bool) {
        loop {
            if self.aborted.get() {
                return;
            }
            if let Some((task, kind)) = self.runtime.next_wake() {
                self.handle_wake(task, kind);
                continue;
            }

            // Finish all runnable tasks before sending the current tag's frame.
            if self.user_tasks_finished() {
                self.shut_down();
            }

            self.complete_tag();

            let next = self.next_safe_tag();
            if self.advance_if_due(next) {
                continue;
            }
            if done() {
                return;
            }

            let alarm_due = match next {
                Some(tag) => self.due(tag),
                None => None,
            };
            if let Some((task, kind)) = self.runtime.wait_wake_until(alarm_due) {
                self.handle_wake(task, kind);
            }
        }
    }

    /// Apply the wake policy before polling a task.
    fn handle_wake(&self, task: TaskId, kind: WakeKind) {
        if self.runtime.finished(task) {
            return; // Ignore stale wakes from tasks that already finished.
        }
        match kind {
            WakeKind::Physical => self.defer(task),
            WakeKind::Resume => {
                // Handle the case when a task is waiting on multiple futures. If any of these are physical, make the task physical.
                if self.foreign.release(task) {
                    // This task may observe its foreign result now, so mark this tag physical.
                    self.physical.set(true);
                }
                self.runtime.poll(task);
            }
        }
    }

    /// Assign a physical resumption tag to a task woken by a foreign future.
    fn defer(&self, task: TaskId) {
        self.foreign.defer(task, || {
            (
                self.mint_physical_tag(),
                self.runtime.waker_for(task, WakeKind::Resume),
            )
        });
    }
}

#[cfg(test)]
impl NodeScheduler {
    /// Run one user task with no frame-transfer work.
    pub(crate) fn run_user(&self, task: impl Future<Output = ()> + 'static) {
        self.run(async {}, vec![Box::pin(task)]);
    }

    /// Run until no task is runnable and no tag can advance immediately.
    pub(crate) fn run_until_idle(&self) {
        self.run_until(|| true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::port::{Builder, InputChannel, InputPort, PortId};
    use crate::time::{MS, S};
    use futures::channel::oneshot;
    use futures::future::{Either, select};
    use std::time::Duration;

    struct FixedSource {
        ent: Cell<Time>,           // The source's logical ENT promise.
        ready: Cell<Option<Time>>, // The next event available for delivery.
        physical: bool,
    }
    impl FixedSource {
        fn new(ent: Time, ready: bool) -> Rc<Self> {
            Rc::new(Self {
                ent: Cell::new(ent),
                ready: Cell::new(ready.then_some(ent)),
                physical: false,
            })
        }
        /// Create a physical event that is excluded from ENT promises.
        fn physical(tag: Time) -> Rc<Self> {
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

    #[test]
    fn ent_is_the_min_over_all_event_sources() {
        let scheduler =
            NodeScheduler::with_execution_mode(Rc::new(Runtime::new()), ExecutionMode::FastForward);
        assert_eq!(scheduler.ent(), Time::FOREVER);
        scheduler.add_source(FixedSource::new(5 * S, true));
        // An empty source's ENT still constrains the node.
        scheduler.add_source(FixedSource::new(2 * S, false));
        assert_eq!(scheduler.ent(), 2 * S);
    }

    #[test]
    fn try_advance_delivers_the_earliest_consumable() {
        let scheduler =
            NodeScheduler::with_execution_mode(Rc::new(Runtime::new()), ExecutionMode::FastForward);
        // One source already has a value at 1s.
        scheduler.add_source(FixedSource::new(1 * S, true));
        // Another source rules out values before 2s.
        scheduler.add_source(FixedSource::new(2 * S, false));

        assert!(scheduler.try_advance());
        assert_eq!(scheduler.now(), 1 * S); // The 1s event is earlier than every unresolved ENT.
    }

    #[test]
    fn try_advance_blocks_on_an_equal_possibility() {
        let scheduler =
            NodeScheduler::with_execution_mode(Rc::new(Runtime::new()), ExecutionMode::FastForward);
        // The left source already has its 2s event.
        scheduler.add_source(FixedSource::new(2 * S, true));
        let announced = FixedSource::new(2 * S, false); // The right source may still deliver at 2s.
        scheduler.add_source(announced.clone());

        assert!(!scheduler.try_advance());
        // The right source has not resolved its 2s event yet.
        assert_eq!(scheduler.now(), Time::ZERO);

        announced.ready.set(Some(2 * S)); // The right source's simultaneous event has arrived.
        assert!(scheduler.try_advance());
        assert_eq!(scheduler.now(), 2 * S); // Both sources can now deliver at the same tag.
    }

    #[test]
    fn closed_upstream_does_not_veto() {
        let scheduler =
            NodeScheduler::with_execution_mode(Rc::new(Runtime::new()), ExecutionMode::FastForward);
        // This source has closed and has no queued values.
        scheduler.add_source(FixedSource::new(Time::FOREVER, false));
        scheduler.add_source(FixedSource::new(3 * S, true)); // Another source has a value at 3s.

        scheduler.try_advance();
        assert_eq!(scheduler.now(), 3 * S); // The closed source cannot block the 3s event.
    }

    struct RecordingSink {
        sent: RefCell<Vec<(Time, Time, bool)>>,
        closed: Cell<bool>,
    }
    impl RecordingSink {
        fn new() -> Rc<Self> {
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

    #[test]
    fn a_tag_is_completed_once() {
        let scheduler =
            NodeScheduler::with_execution_mode(Rc::new(Runtime::new()), ExecutionMode::FastForward);
        let sink = RecordingSink::new();
        scheduler.add_sink(sink.clone());
        let source = FixedSource::new(1 * S, true);
        scheduler.add_source(source.clone());

        scheduler.try_advance();
        assert!(sink.sent.borrow().is_empty()); // A tag's frame is sent after its tasks run.
        assert_eq!(scheduler.now(), 1 * S);

        source.ent.set(Time::FOREVER); // The source has no remaining logical events.
        source.ready.set(None);
        scheduler.try_advance();
        assert_eq!(*sink.sent.borrow(), vec![(1 * S, Time::FOREVER, false)]);

        scheduler.try_advance();
        assert_eq!(sink.sent.borrow().len(), 1); // A completed tag never sends a second frame.
    }

    #[test]
    fn shutting_down_closes_each_drain() {
        let scheduler =
            NodeScheduler::with_execution_mode(Rc::new(Runtime::new()), ExecutionMode::FastForward);
        let sink = RecordingSink::new();
        scheduler.add_sink(sink.clone());
        scheduler.shut_down();
        assert!(sink.closed.get());
    }

    #[test]
    fn an_elapsed_wait_is_delivered_before_later_inputs() {
        let scheduler =
            NodeScheduler::with_execution_mode(Rc::new(Runtime::new()), ExecutionMode::FastForward);
        let alarms = Rc::new(Alarms::new());
        let alarm = alarms.set(700 * MS, false);
        scheduler.add_source(alarms.clone());
        scheduler.add_source(FixedSource::new(1 * S, true));
        scheduler.add_source(FixedSource::new(2 * S, false));

        scheduler.try_advance();
        assert_eq!(scheduler.now(), 700 * MS);
        assert!(!scheduler.is_physical()); // This alarm was created at a logical tag.

        drop(alarm); // Completing the wait removes its alarm.
        scheduler.try_advance();
        assert_eq!(scheduler.now(), 1 * S);
    }

    #[test]
    #[should_panic(expected = "ENT would be less than timestamp")]
    fn an_unconsumed_event_at_the_idle_point_is_a_scheduling_bug() {
        let scheduler =
            NodeScheduler::with_execution_mode(Rc::new(Runtime::new()), ExecutionMode::FastForward);
        let alarms = Rc::new(Alarms::new());
        let _unconsumed = alarms.set(700 * MS, false);
        scheduler.add_source(alarms.clone());

        scheduler.try_advance();
        assert_eq!(scheduler.now(), 700 * MS);
        // An unconsumed alarm would make ENT equal the completed tag, violating ENT > now.
        scheduler.try_advance();
    }

    #[test]
    fn concurrent_waits_deliver_in_tag_order() {
        let scheduler =
            NodeScheduler::with_execution_mode(Rc::new(Runtime::new()), ExecutionMode::FastForward);
        let alarms = Rc::new(Alarms::new());
        let first = alarms.set(700 * MS, false);
        let second = alarms.set(1 * S, false);
        scheduler.add_source(alarms.clone());
        scheduler.add_source(FixedSource::new(2 * S, false));

        scheduler.try_advance();
        assert_eq!(scheduler.now(), 700 * MS);

        drop(first);
        scheduler.try_advance();
        assert_eq!(scheduler.now(), 1 * S);

        drop(second);
        scheduler.try_advance();
        assert_eq!(scheduler.now(), 1 * S); // No remaining source has an event to deliver.
    }

    #[test]
    fn try_advance_without_a_ready_head_is_a_no_op() {
        let scheduler =
            NodeScheduler::with_execution_mode(Rc::new(Runtime::new()), ExecutionMode::FastForward);
        assert!(!scheduler.try_advance());
        assert_eq!(scheduler.now(), Time::ZERO); // No source has a pending event.

        // This source has an ENT promise but no event.
        scheduler.add_source(FixedSource::new(5 * S, false));
        assert!(!scheduler.try_advance());
        assert_eq!(scheduler.now(), Time::ZERO);
    }

    #[test]
    fn a_physical_tag_behind_the_node_is_the_tag_right_after_now() {
        let scheduler =
            NodeScheduler::with_execution_mode(Rc::new(Runtime::new()), ExecutionMode::FastForward);
        scheduler.advance_to(5 * S); // Logical time runs ahead of the wall clock.
        assert_eq!(scheduler.mint_physical_tag(), (5 * S).next_microstep());
    }

    #[test]
    fn physical_now_runs_on_the_wall_clock() {
        let scheduler =
            NodeScheduler::with_execution_mode(Rc::new(Runtime::new()), ExecutionMode::FastForward);
        scheduler.start();
        let first = scheduler.physical_now();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let second = scheduler.physical_now();
        assert!(second >= first + 2 * MS, "{second} < {first} + 2ms");
        // Elapsed wall-clock time does not advance logical time.
        assert_eq!(scheduler.now(), Time::ZERO);
    }

    #[test]
    fn a_physical_source_is_delivered_but_never_promised() {
        let scheduler =
            NodeScheduler::with_execution_mode(Rc::new(Runtime::new()), ExecutionMode::FastForward);
        let sink = RecordingSink::new();
        scheduler.add_sink(sink.clone());
        let action = FixedSource::physical(3 * S);
        scheduler.add_source(action.clone());
        // An empty input promises no logical values before 5s.
        scheduler.add_source(FixedSource::new(5 * S, false));
        assert_eq!(scheduler.ent(), 5 * S); // Physical events do not lower the node's ENT.

        scheduler.try_advance();
        assert_eq!(scheduler.now(), 3 * S); // Physical events still advance logical time.

        action.ready.set(None); // The physical event has been consumed.
        scheduler.try_advance();
        // The output frame records the tag's physical origin.
        assert_eq!(*sink.sent.borrow(), vec![(3 * S, 5 * S, true)]);
    }

    /// Create a FastForward node with a sink that records its output frames.
    fn node_rig() -> (Rc<Runtime>, Rc<NodeScheduler>, Rc<RecordingSink>) {
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

    fn input(scheduler: &Rc<NodeScheduler>) -> (InputPort<i64>, Rc<InputChannel<i64>>) {
        let channel = Rc::new(InputChannel::<i64>::new());
        scheduler.add_source(channel.clone());
        let port = InputPort {
            id: PortId {
                node_id: 0,
                port_id: 0,
            },
            channel: channel.clone(),
            scheduler: scheduler.clone(),
        };
        (port, channel)
    }

    /// Record whether a wait preserves the physical flag of its triggering input.
    fn wait_after(physical: bool) -> ((bool, bool), Vec<(Time, Time, bool)>) {
        let (_runtime, scheduler, sink) = node_rig();
        let (port, channel) = input(&scheduler);
        let node = Node {
            scheduler: scheduler.clone(),
        };
        if physical {
            channel.push_physical(2 * MS, 7);
        } else {
            channel.push(2 * MS, 7);
        }
        channel.update_remote_ent(Time::FOREVER);

        let seen = Rc::new(Cell::new(None));
        let (out, task_scheduler) = (seen.clone(), scheduler.clone());
        scheduler.run_user(async move {
            port.wait().await;
            let armed_at = task_scheduler.is_physical();
            node.wait(1 * MS).await;
            out.set(Some((armed_at, task_scheduler.is_physical())));
        });
        (seen.get().unwrap(), sink.sent.borrow().clone())
    }

    #[test]
    fn a_wait_armed_at_a_physical_tag_elapses_physically() {
        let (flags, frames) = wait_after(true);
        assert_eq!(flags, (true, true));
        // Both frames are physical and promise no further logical events.
        assert_eq!(
            frames,
            vec![(2 * MS, Time::FOREVER, true), (3 * MS, Time::FOREVER, true)]
        );
    }

    #[test]
    fn a_wait_armed_at_a_logical_tag_stays_logical() {
        let (flags, frames) = wait_after(false);
        assert_eq!(flags, (false, false));
        assert_eq!(
            frames,
            vec![(2 * MS, 3 * MS, false), (3 * MS, Time::FOREVER, false)]
        );
    }

    fn send_later(value: i64) -> (oneshot::Receiver<i64>, std::thread::JoinHandle<()>) {
        let (done, result) = oneshot::channel();
        let worker = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(5));
            done.send(value).unwrap();
        });
        (result, worker)
    }

    #[test]
    fn a_foreign_future_resumes_at_a_fresh_physical_tag() {
        let (_runtime, scheduler, sink) = node_rig();
        let node = Node {
            scheduler: scheduler.clone(),
        };
        let (result, worker) = send_later(7);

        let seen = Rc::new(Cell::new(None));
        let (out, task_scheduler) = (seen.clone(), scheduler.clone());
        scheduler.run_user(async move {
            // Begin at a logical tag before awaiting the foreign result.
            let started = node.wait(1 * MS).await;
            // The foreign future uses the task's default Physical waker.
            let value = result.await.unwrap();
            out.set(Some((
                started,
                value,
                task_scheduler.now(),
                task_scheduler.is_physical(),
            )));
        });
        worker.join().unwrap();

        let (started, value, resumed, physical) = seen.get().unwrap();
        assert_eq!((started, value, physical), (1 * MS, 7, true));
        assert!(resumed > started);
        // A foreign result creates a physical tag without lowering the logical ENT.
        assert_eq!(
            *sink.sent.borrow(),
            vec![
                (1 * MS, Time::FOREVER, false),
                (resumed, Time::FOREVER, true)
            ]
        );
    }

    #[test]
    fn a_pending_foreign_future_does_not_hold_time() {
        let (_runtime, scheduler, sink) = node_rig();
        let ticker = Node {
            scheduler: scheduler.clone(),
        };
        let (done, result) = oneshot::channel::<i64>();

        let seen = Rc::new(Cell::new(None));
        let (out, task_scheduler) = (seen.clone(), scheduler.clone());
        scheduler.spawn(async move {
            let value = result.await.unwrap();
            out.set(Some((
                task_scheduler.now(),
                task_scheduler.is_physical(),
                value,
            )));
        });
        scheduler.run_user(async move {
            // Other tasks can advance logical time while this result is pending.
            ticker.wait(1 * MS).await;
            ticker.wait(1 * MS).await;
            done.send(7).unwrap(); // The waiting task still treats this completion as physical.
        });

        let (resumed, physical, value) = seen.get().unwrap();
        assert!(resumed > 2 * MS, "{resumed}");
        assert_eq!((physical, value), (true, 7));
        assert_eq!(
            *sink.sent.borrow(),
            vec![
                (1 * MS, 2 * MS, false),
                (2 * MS, Time::FOREVER, false),
                (resumed, Time::FOREVER, true)
            ]
        );
    }

    #[test]
    fn a_logical_event_reaching_a_deferred_task_makes_its_tag_physical() {
        let (runtime, scheduler, sink) = node_rig();
        let (port, channel) = input(&scheduler);
        let (result, worker) = send_later(7);
        // The later-arriving input has an earlier tag than the deferred foreign resumption.
        let (after, then) = oneshot::channel::<()>();
        let later = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(15));
            after.send(()).unwrap();
        });
        runtime.spawn_internal(async move {
            then.await.unwrap();
            channel.push(1 * MS, 5);
            channel.update_remote_ent(Time::FOREVER);
        });

        let seen = Rc::new(Cell::new(None));
        let (out, task_scheduler) = (seen.clone(), scheduler.clone());
        scheduler.run_user(async move {
            let arrived = match select(port.pin(), result).await {
                Either::Left((value, _)) => *value.unwrap().0,
                Either::Right(_) => unreachable!("the value is delivered first"),
            };
            out.set(Some((
                arrived,
                task_scheduler.now(),
                task_scheduler.is_physical(),
            )));
        });
        worker.join().unwrap();
        later.join().unwrap();

        // The task can observe its foreign result at the input's tag.
        assert_eq!(seen.get(), Some((5, 1 * MS, true)));
        // The input wake cancels the task's separate physical resumption tag.
        assert_eq!(*sink.sent.borrow(), vec![(1 * MS, Time::FOREVER, true)]);
        assert_eq!(scheduler.now(), 1 * MS);
    }

    /// Yield once by requesting another poll.
    async fn yield_once() {
        let mut yielded = false;
        std::future::poll_fn(|cx| {
            if yielded {
                return Poll::Ready(());
            }
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        })
        .await
    }

    #[test]
    fn the_node_advances_only_once_no_task_can_run() {
        let (runtime, scheduler, _sink) = node_rig();
        let node = Node {
            scheduler: scheduler.clone(),
        };
        let tags = Rc::new(RefCell::new(Vec::new()));
        for _ in 0..2 {
            let (tags, scheduler) = (tags.clone(), scheduler.clone());
            runtime.spawn_internal(async move {
                tags.borrow_mut().push(scheduler.now());
                yield_once().await; // A queued wake keeps the current tag open.
                tags.borrow_mut().push(scheduler.now());
            });
        }
        let woke_at = Rc::new(Cell::new(None));
        let out = woke_at.clone();
        scheduler.run_user(async move { out.set(Some(node.wait(1 * MS).await)) });
        // Every runnable task finished before logical time advanced.
        assert_eq!(*tags.borrow(), vec![Time::ZERO; 4]);
        assert_eq!(woke_at.get(), Some(1 * MS));
    }

    #[test]
    fn every_due_tag_is_completed_before_the_node_waits() {
        let (_runtime, scheduler, sink) = node_rig();
        let (_port, channel) = input(&scheduler);
        channel.push(1 * S, 1); // Delivery must advance time even when no task is waiting.
        channel.push(2 * S, 2);
        channel.update_remote_ent(Time::FOREVER);
        let (result, worker) = send_later(7);
        scheduler.run_user(async move {
            result.await.unwrap();
        });
        worker.join().unwrap();

        // Both queued tags finish before the foreign result arrives.
        let frames = sink.sent.borrow();
        assert_eq!(
            frames[..2],
            [(1 * S, 2 * S, false), (2 * S, Time::FOREVER, false)]
        );
        // The foreign result resumes after both queued tags.
        assert!(frames[2].0 > 2 * S, "{}", frames[2].0);
    }

    #[test]
    fn a_foreign_wake_of_an_internal_task_runs_at_once() {
        let (runtime, scheduler, _sink) = node_rig();
        let (result, worker) = send_later(7);
        let seen = Rc::new(Cell::new(None));
        let (out, task_scheduler) = (seen.clone(), scheduler.clone());
        let (done, finished) = oneshot::channel();
        runtime.spawn_internal(async move {
            let value = result.await.unwrap();
            out.set(Some((
                value,
                task_scheduler.now(),
                task_scheduler.is_physical(),
            )));
            done.send(()).unwrap();
        });
        scheduler.run(async move { finished.await.unwrap() }, Vec::new());
        worker.join().unwrap();
        // Internal tasks resume without creating a new tag.
        assert_eq!(seen.get(), Some((7, Time::ZERO, false)));
    }

    #[test]
    fn a_resume_waker_woken_from_a_thread_is_not_deferred() {
        let (_runtime, scheduler, _sink) = node_rig();
        let seen = Rc::new(Cell::new(None));
        let (out, task_scheduler) = (seen.clone(), scheduler.clone());
        scheduler.run_user(async move {
            let mut handed_out = false;
            std::future::poll_fn(|cx| {
                if handed_out {
                    return Poll::Ready(());
                }
                handed_out = true;
                let resume = Runtime::waker(cx, WakeKind::Resume);
                std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_millis(5));
                    resume.wake();
                });
                Poll::Pending
            })
            .await;
            out.set(Some((task_scheduler.now(), task_scheduler.is_physical())));
        });
        // Resume wakes poll at the current tag without physical deferral.
        assert_eq!(seen.get(), Some((Time::ZERO, false)));
    }

    #[test]
    #[should_panic(expected = "already completed")]
    fn setting_an_output_at_the_start_tag_is_a_bug() {
        let scheduler = Rc::new(NodeScheduler::with_execution_mode(
            Rc::new(Runtime::new()),
            ExecutionMode::FastForward,
        ));
        let mut builder = Builder::new(0, scheduler);
        let out = builder.output::<i64>();
        out.set(1); // The initial tag is already complete, so setting output must fail.
    }

    #[test]
    fn a_pending_physical_wait_is_delivered_at_its_own_tag() {
        let scheduler =
            NodeScheduler::with_execution_mode(Rc::new(Runtime::new()), ExecutionMode::FastForward);
        let _alarm = scheduler.alarms.set(2 * S, true);
        scheduler.add_source(FixedSource::new(5 * S, true));
        assert_eq!(scheduler.ent(), 5 * S); // Physical alarms are excluded from ENT.

        scheduler.try_advance();
        // The physical alarm executes before the later logical event.
        assert_eq!(scheduler.now(), 2 * S);
    }

    #[test]
    fn builder_execution_mode_controls_wait_pacing() {
        let runtime = Rc::new(Runtime::new());
        let scheduler = Rc::new(NodeScheduler::new(runtime));
        let mut builder = Builder::new(0, scheduler.clone());
        let node = builder.node();
        let seen = Rc::new(Cell::new(None));
        let out = seen.clone();
        let target = 3600 * S;
        scheduler.spawn(async move {
            out.set(Some(node.wait(target).await));
        });

        scheduler.run_until_idle();
        assert_eq!(seen.get(), None); // Realtime mode waits for the wall clock.
        assert_eq!(scheduler.now(), Time::ZERO);

        builder.execution_mode(ExecutionMode::FastForward);
        scheduler.run_until_idle();
        assert_eq!(seen.get(), Some(target)); // FastForward mode removes the wall-clock wait.
        assert_eq!(scheduler.now(), target);
    }

    #[test]
    fn the_default_mode_lets_physical_time_reach_the_tag() {
        let scheduler = NodeScheduler::new(Rc::new(Runtime::new()));
        let _alarm = scheduler.alarms.set(2 * S, false);

        assert!(!scheduler.try_advance());
        // The tag is not yet due on the wall clock.
        assert_eq!(scheduler.now(), Time::ZERO);

        scheduler
            .start_time
            .set(Instant::now() - Duration::from_secs(2));
        assert!(scheduler.try_advance());
        assert_eq!(scheduler.now(), 2 * S);
    }

    #[test]
    fn a_realtime_node_sleeps_until_the_tag() {
        let (_runtime, scheduler, _sink) = node_rig();
        scheduler.set_execution_mode(ExecutionMode::Realtime);
        let node = Node {
            scheduler: scheduler.clone(),
        };
        let start = Instant::now();
        scheduler.run_user(async move {
            node.wait(5 * MS).await;
        });
        assert!(start.elapsed() >= Duration::from_millis(5));
    }

    #[test]
    fn an_unrelated_advance_does_not_absorb_a_deferred_task() {
        let (runtime, scheduler, _sink) = node_rig();
        let (a, a_channel) = input(&scheduler);
        let (_b, b_channel) = input(&scheduler);
        // This input has no logical events but remains open.
        a_channel.update_remote_ent(Time::FOREVER);
        // The result arrives after 5ms and gets a physical resumption tag.
        let (result, worker) = send_later(7);
        // B arrives later in wall-clock time but has an earlier logical tag.
        let (after, then) = oneshot::channel::<()>();
        let later = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(15));
            after.send(()).unwrap();
        });
        runtime.spawn_internal(async move {
            then.await.unwrap();
            b_channel.push(3 * MS, 1);
            b_channel.update_remote_ent(Time::FOREVER);
        });

        let seen = Rc::new(Cell::new(None));
        let (out, task_scheduler) = (seen.clone(), scheduler.clone());
        scheduler.run_user(async move {
            let value = match select(a.pin(), result).await {
                Either::Left(_) => unreachable!("A never delivers"),
                Either::Right((value, _)) => value.unwrap(),
            };
            out.set(Some((
                value,
                task_scheduler.now(),
                task_scheduler.is_physical(),
            )));
        });
        worker.join().unwrap();
        later.join().unwrap();

        let (value, resumed_at, physical) = seen.get().unwrap();
        assert_eq!((value, physical), (7, true));
        assert!(
            resumed_at > 3 * MS,
            "resumed at {resumed_at}: B's tag, where nothing of A's arrived"
        );
    }

    #[test]
    fn await_physical_continues_physically_even_when_already_complete() {
        let (_runtime, scheduler, sink) = node_rig();
        let node = Node {
            scheduler: scheduler.clone(),
        };
        let (done, result) = oneshot::channel::<i64>();
        // The future is already ready, so awaiting it will not register a wake.
        done.send(7).unwrap();
        let seen = Rc::new(Cell::new(None));
        let (out, task_scheduler) = (seen.clone(), scheduler.clone());
        scheduler.run_user(async move {
            node.wait(1 * MS).await;
            let value = node.await_physical(result).await.unwrap();
            out.set(Some((
                value,
                task_scheduler.now(),
                task_scheduler.is_physical(),
            )));
        });
        let (value, at, physical) = seen.get().unwrap();
        assert_eq!((value, physical), (7, true));
        assert!(at > 1 * MS, "{at}");
        assert_eq!(
            *sink.sent.borrow(),
            vec![(1 * MS, Time::FOREVER, false), (at, Time::FOREVER, true)]
        );
    }

    #[test]
    fn a_task_spawned_by_a_task_keeps_the_node_running() {
        let (_runtime, scheduler, sink) = node_rig();
        let node = Rc::new(Node {
            scheduler: scheduler.clone(),
        });
        let seen = Rc::new(Cell::new(None));
        let (out, parent) = (seen.clone(), node.clone());
        scheduler.run_user(async move {
            let child = parent.clone();
            parent.spawn(async move { out.set(Some(child.wait(2 * MS).await)) });
            parent.wait(1 * MS).await;
        });
        assert_eq!(seen.get(), Some(2 * MS));
        // The outputs stay open until the spawned task finishes.
        assert_eq!(
            *sink.sent.borrow(),
            vec![(1 * MS, 2 * MS, false), (2 * MS, Time::FOREVER, false)]
        );
        assert!(sink.closed.get());
    }

    #[test]
    fn an_aborted_node_stops_without_completing_its_tag() {
        let (_runtime, scheduler, sink) = node_rig();
        let node = Node {
            scheduler: scheduler.clone(),
        };
        let late = Rc::new(Cell::new(false));
        let (out, other) = (late.clone(), scheduler.clone());
        scheduler.spawn(async move {
            Node { scheduler: other }.wait(5 * MS).await;
            out.set(true);
        });
        scheduler.run_user(async move {
            node.wait(1 * MS).await;
            assert!(!node.tag_complete());
            node.abort();
        });
        assert_eq!(scheduler.now(), 1 * MS);
        assert!(!late.get()); // The other task never ran again.
        assert!(sink.sent.borrow().is_empty());
        assert!(!sink.closed.get());
    }

    #[test]
    fn the_clock_starts_at_zero_and_jumps() {
        let scheduler =
            NodeScheduler::with_execution_mode(Rc::new(Runtime::new()), ExecutionMode::FastForward);
        assert_eq!(scheduler.now(), Time::ZERO);
        scheduler.advance_to(Time::new(10));
        assert_eq!(scheduler.now(), Time::new(10));
    }

    #[test]
    #[should_panic(expected = "cannot advance")]
    fn the_clock_is_monotonic() {
        let scheduler =
            NodeScheduler::with_execution_mode(Rc::new(Runtime::new()), ExecutionMode::FastForward);
        scheduler.advance_to(Time::new(10));
        scheduler.advance_to(Time::new(3));
    }

    #[test]
    #[should_panic(expected = "cannot advance")]
    fn the_clock_rejects_the_current_tag() {
        let scheduler =
            NodeScheduler::with_execution_mode(Rc::new(Runtime::new()), ExecutionMode::FastForward);
        scheduler.advance_to(Time::new(10));
        scheduler.advance_to(Time::new(10)); // Advancing to the current tag must fail.
    }
}
