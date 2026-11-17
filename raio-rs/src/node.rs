use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::task::{Poll, Waker};
use std::time::Instant;

use crate::runtime::{BoxFuture, Runtime, TaskId, WakeKind};
use crate::time::Time;
#[cfg(test)]
use futures::channel::oneshot;

pub type NodeId = usize;

pub trait EventSource {
    /// Earliest Next Timestamp: The earliest tag at which this source can still produce a logical event.
    /// Promises never to produce logical events with timestamp < ent().
    fn ent(&self) -> Time;

    /// Next Ready Timestamp: timestamp of the next ready to be consumed event.
    /// None if next event is unknown. Promises never to produce any events with timestamp < ready().
    fn ready(&self) -> Option<Time>;

    /// The earliest tag at which this source can still produce any event, logical or
    /// physical: the node must not advance past it. Physical events are left out of the
    /// promise `ent()` but not out of this.
    fn earliest_event(&self) -> Time {
        self.ent()
    }

    /// Execute events with timestamp <= time. Trigger wakers registered in this source.
    /// Returns true if any physical events were executed.
    fn wake_wakers(&self, time: Time) -> bool;
}

pub trait EventDrain {
    fn send(&self, timestamp: Time, ent: Time, physical: bool);
    fn close(&self);
}

pub struct Timer {
    entries: RefCell<Vec<Weak<TimerEntry>>>,
    /// Woken by `arm` so it can watch the new entry.
    watcher: RefCell<Option<Waker>>,
}

pub(crate) struct TimerEntry {
    target: Time,
    due: Instant,   // the wall-clock instant at which `target` is reached
    physical: bool, // armed at a physical tag: elapses physically
    waker: RefCell<Option<Waker>>,
}

impl Timer {
    pub(crate) fn new() -> Self {
        Self {
            entries: RefCell::new(Vec::new()),
            watcher: RefCell::new(None),
        }
    }

    pub(crate) fn arm(&self, target: Time, due: Instant, physical: bool) -> Rc<TimerEntry> {
        let entry = Rc::new(TimerEntry {
            target,
            due,
            physical,
            waker: RefCell::new(None),
        });
        let mut entries = self.entries.borrow_mut();
        entries.retain(|entry| entry.strong_count() > 0);
        entries.push(Rc::downgrade(&entry));
        drop(entries);
        if let Some(watcher) = self.watcher.borrow().as_ref() {
            watcher.wake_by_ref();
        }
        entry
    }

    /// Wakes the node as the wall clock reaches each entry, so the idle point delivers it.
    /// A waiting task is never woken by the wall clock, only by the scheduler at its tag.
    pub(crate) fn watch_timers(timer: Rc<Timer>) -> impl Future<Output = ()> {
        let mut alarm = async_io::Timer::never();
        std::future::poll_fn(move |cx| {
            loop {
                *timer.watcher.borrow_mut() = Some(cx.waker().clone()); // `arm` wakes the watcher
                let now = Instant::now();
                let pending = timer
                    .live()
                    .iter()
                    .map(|entry| entry.due)
                    .filter(|due| *due > now)
                    .min();
                let Some(due) = pending else {
                    return Poll::Pending;
                };
                alarm.set_at(due);
                if Pin::new(&mut alarm).poll(cx).is_pending() {
                    return Poll::Pending;
                }
                // The alarm elapsed: the next idle point sees the entry due. Find the next one.
            }
        })
    }

    fn live(&self) -> Vec<Rc<TimerEntry>> {
        self.entries
            .borrow()
            .iter()
            .filter_map(Weak::upgrade)
            .collect()
    }
}

impl EventSource for Timer {
    fn ent(&self) -> Time {
        self.live()
            .iter()
            .filter(|entry| !entry.physical)
            .map(|entry| entry.target)
            .min()
            .unwrap_or(Time::FOREVER)
    }

    /// The earliest entry, once the wall clock has reached it.
    fn ready(&self) -> Option<Time> {
        let live = self.live();
        let min = live.iter().map(|entry| entry.target).min()?;
        let now = Instant::now();
        live.iter()
            .any(|entry| entry.target == min && entry.due <= now)
            .then_some(min)
    }

    fn earliest_event(&self) -> Time {
        self.live()
            .iter()
            .map(|entry| entry.target)
            .min()
            .unwrap_or(Time::FOREVER)
    }

    fn wake_wakers(&self, time: Time) -> bool {
        let mut physical = false;
        for entry in self.live() {
            if entry.target <= time {
                physical |= entry.physical;
                if let Some(waker) = entry.waker.borrow_mut().take() {
                    waker.wake();
                }
            }
        }
        physical
    }
}

pub struct Node {
    pub(crate) context: Rc<NodeContext>,
    pub(crate) timer: Rc<Timer>,
}

impl Node {
    pub fn now(&self) -> Time {
        self.context.now()
    }

    /// Whether the current tag was caused by a physical event.
    pub fn physical(&self) -> bool {
        self.context.physical.get()
    }

    pub async fn wait(&self, duration: Time) -> Time {
        assert!(duration > Time::ZERO, "wait needs a positive duration");
        let target = self.context.now() + duration;
        let due = self.context.start_time.get() + target.duration();
        let entry = self.timer.arm(target, due, self.context.physical.get());
        // Woken by the scheduler when it reaches `target`.
        let logical = Runtime::waker(WakeKind::Logical);
        std::future::poll_fn(|_| {
            if self.context.now() >= target {
                return Poll::Ready(());
            }
            *entry.waker.borrow_mut() = Some(logical.clone());
            Poll::Pending
        })
        .await;
        self.context.now()
    }

    /// Await `future`, then continue at a fresh physical tag even if it was already
    /// complete. A foreign future that completes before its first poll never wakes the
    /// task, so the runtime cannot tell its result from a logical one; use this for a
    /// result whose timing came from outside logical time, such as a thread's.
    pub async fn await_physical<F: Future>(&self, future: F) -> F::Output {
        let output = future.await;
        let mut deferred = false;
        std::future::poll_fn(|_| {
            if deferred {
                return Poll::Ready(());
            }
            deferred = true;
            self.context.defer(Runtime::current_task());
            Poll::Pending
        })
        .await;
        output
    }
}

/// Tasks woken by foreign futures, each waiting for its physical tag.
struct ForeignSource {
    pending: RefCell<HashMap<TaskId, (Time, Waker)>>,
}

impl EventSource for ForeignSource {
    fn ent(&self) -> Time {
        Time::FOREVER // never promised
    }

    fn ready(&self) -> Option<Time> {
        self.pending.borrow().values().map(|(tag, _)| *tag).min()
    }

    fn wake_wakers(&self, time: Time) -> bool {
        let due: Vec<_> = self
            .pending
            .borrow_mut()
            .extract_if(|_, (tag, _)| *tag <= time)
            .collect();
        for (_, (_, resume)) in &due {
            resume.wake_by_ref();
        }
        !due.is_empty()
    }
}

pub struct NodeContext {
    runtime: Rc<Runtime>,
    now: Cell<Time>,
    pub(crate) start_time: Cell<Instant>,
    pub(crate) event_sources: RefCell<Vec<Rc<dyn EventSource>>>,
    pub(crate) event_drains: RefCell<Vec<Rc<dyn EventDrain>>>,
    /// The current tag's frame has been sent: nothing can be added to the tag.
    pub(crate) tag_complete: Cell<bool>,
    /// The current tag was caused by a physical event.
    pub(crate) physical: Cell<bool>,
    foreign: Rc<ForeignSource>,
}

impl NodeContext {
    pub fn new(runtime: Rc<Runtime>) -> Self {
        let foreign = Rc::new(ForeignSource {
            pending: RefCell::new(HashMap::new()),
        });
        Self {
            runtime,
            now: Cell::new(Time::ZERO),
            start_time: Cell::new(Instant::now()),
            event_sources: RefCell::new(vec![foreign.clone()]),
            event_drains: RefCell::new(Vec::new()),
            tag_complete: Cell::new(true),
            physical: Cell::new(false),
            foreign,
        }
    }

    pub fn now(&self) -> Time {
        self.now.get()
    }

    pub fn physical_now(&self) -> Time {
        Time::new(self.start_time.get().elapsed().as_nanos() as i64)
    }

    /// The tag for a physical event happening now: the wall-clock time, or the tag right
    /// after `now` if the node has already passed it.
    pub fn physical_tag(&self) -> Time {
        self.physical_now().max(self.now().next_microstep())
    }

    /// Move the clock to `time` and deliver every event at or before it.
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

    pub fn send_events(&self, timestamp: Time, ent: Time) {
        if self.tag_complete.get() {
            return;
        }
        assert!(
            ent > timestamp,
            "ENT would be less than timestamp: {ent} < {timestamp}"
        );
        for drain in self.event_drains.borrow().iter() {
            drain.send(timestamp, ent, self.physical.get());
        }
        self.tag_complete.set(true);
    }

    /// The node's own tasks have all finished: its last frame promises silence, and its
    /// outputs close.
    fn shut_down(&self) {
        self.send_events(self.now(), Time::FOREVER);
        for drain in self.event_drains.borrow_mut().drain(..) {
            drain.close();
        }
    }

    /// Earliest Next Timestamp: the timestamp of the earliest next event that this node will produce.
    /// New timestamp t >= ent().
    pub fn ent(&self) -> Time {
        self.event_sources
            .borrow()
            .iter()
            .map(|source| source.ent())
            .min()
            .unwrap_or(Time::FOREVER)
    }

    pub fn try_advance(&self) {
        self.send_events(self.now(), self.ent());

        let (next, stat) = {
            let sources = self.event_sources.borrow();
            // Asked once per source: readiness follows the wall clock.
            let ready: Vec<Option<Time>> = sources.iter().map(|source| source.ready()).collect();
            // The earliest event a source already holds: the candidate next tag.
            let next = ready.iter().flatten().min().copied();
            // The earliest event a source that holds nothing could still produce.
            let stat = sources
                .iter()
                .zip(&ready)
                .filter(|(_, ready)| ready.is_none())
                .map(|(source, _)| source.earliest_event())
                .min();
            (next.unwrap_or(Time::NEVER), stat.unwrap_or(Time::FOREVER))
        };
        if next <= self.now() {
            return;
        }
        if next < stat {
            self.advance_to(next);
        }
    }
}

impl NodeContext {
    /// Run the node until `frame_transfer` and its own `tasks` have all finished. A
    /// foreign wake-up of one of the tasks is not polled but becomes a physical event, so
    /// the task resumes at a fresh tag. `frame_transfer` sends and receives frames and is
    /// never deferred.
    pub(crate) fn run(
        &self,
        frame_transfer: impl Future<Output = ()> + 'static,
        tasks: Vec<BoxFuture<()>>,
    ) {
        let root = self.runtime.spawn_internal(frame_transfer);
        for task in tasks {
            self.runtime.spawn(task);
        }
        self.run_until(|| self.runtime.finished(root) && self.runtime.live_user_tasks() == 0);
    }

    /// The scheduling loop, until `done` holds at an idle point with nothing left to advance.
    fn run_until(&self, done: impl Fn() -> bool) {
        loop {
            let (task, kind) = match self.runtime.next_wake() {
                Some(wake) => wake,
                None => {
                    // No task can run: the current tag is complete.
                    if self.runtime.live_user_tasks() == 0 {
                        self.shut_down(); // once the outputs are closed, this is a no-op
                    }
                    let before = self.now();
                    self.try_advance();
                    if self.now() != before {
                        continue; // poll whatever the advance woke, or complete the new tag
                    }
                    if done() {
                        return;
                    }
                    self.runtime.wait_wake()
                }
            };
            if self.runtime.finished(task) {
                continue; // its waker outlived it
            }
            if kind == WakeKind::Physical {
                self.defer(task);
                continue;
            }
            if self.foreign.pending.borrow_mut().remove(&task).is_some() {
                // Another event arrived before its resumption: the task may observe a
                // foreign result at this tag, so the tag is physical.
                self.physical.set(true);
            }
            self.runtime.poll(task);
        }
    }

    /// A foreign future woke `task`: it resumes at a fresh physical tag instead of now.
    fn defer(&self, task: TaskId) {
        let mut waiting = self.foreign.pending.borrow_mut();
        if !waiting.contains_key(&task) {
            let resume = self.runtime.waker_for(task, WakeKind::Logical);
            waiting.insert(task, (self.physical_tag(), resume));
        }
    }
}

#[cfg(test)]
impl NodeContext {
    /// Run `task` as the node's only task, until it completes.
    pub(crate) fn run_user(&self, task: impl Future<Output = ()> + 'static) {
        self.run(async {}, vec![Box::pin(task)]);
    }

    /// Run until no task can run and no tag can be advanced to.
    pub(crate) fn run_until_idle(&self) {
        self.run_until(|| true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::port::{Builder, InputChannel, InputPort, PortId};
    use crate::time::{MS, S};
    use futures::future::{Either, select};
    use std::cell::Cell;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Wake;
    use std::time::Duration;

    struct FixedSource {
        ent: Cell<Time>,           // the logical promise
        ready: Cell<Option<Time>>, // the deliverable tag, if any
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
        /// A physical event: deliverable at `tag`, never promised.
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
        fn ready(&self) -> Option<Time> {
            self.ready.get()
        }
        fn wake_wakers(&self, _time: Time) -> bool {
            self.physical // nothing to deliver, nobody waiting
        }
    }

    #[test]
    fn ent_is_the_min_over_all_event_sources() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        assert_eq!(ctx.ent(), Time::FOREVER);
        ctx.event_sources
            .borrow_mut()
            .push(FixedSource::new(5 * S, true));
        ctx.event_sources
            .borrow_mut()
            .push(FixedSource::new(2 * S, false)); // possibilities count too
        assert_eq!(ctx.ent(), 2 * S);
    }

    #[test]
    fn try_advance_delivers_the_earliest_consumable() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        ctx.event_sources
            .borrow_mut()
            .push(FixedSource::new(1 * S, true)); // hello's queued frame
        ctx.event_sources
            .borrow_mut()
            .push(FixedSource::new(2 * S, false)); // its piggybacked promise

        ctx.try_advance();
        assert_eq!(ctx.now(), 1 * S); // 1s < promise 2s: delivered
    }

    #[test]
    fn try_advance_blocks_on_an_equal_possibility() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        ctx.event_sources
            .borrow_mut()
            .push(FixedSource::new(2 * S, true)); // left's queued tick
        let announced = FixedSource::new(2 * S, false); // right announced ent=2s
        ctx.event_sources.borrow_mut().push(announced.clone());

        ctx.try_advance();
        assert_eq!(ctx.now(), Time::ZERO); // right's 2s value may be in flight

        announced.ready.set(Some(2 * S)); // it arrived: the simultaneous tick
        ctx.try_advance();
        assert_eq!(ctx.now(), 2 * S); // both delivered at one instant
    }

    #[test]
    fn closed_upstream_does_not_veto() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        ctx.event_sources
            .borrow_mut()
            .push(FixedSource::new(Time::FOREVER, false)); // closed, drained
        ctx.event_sources
            .borrow_mut()
            .push(FixedSource::new(3 * S, true)); // sibling's queued value

        ctx.try_advance();
        assert_eq!(ctx.now(), 3 * S); // FOREVER promise gates nothing
    }

    struct RecordingDrain {
        sent: RefCell<Vec<(Time, Time, bool)>>,
        closed: Cell<bool>,
    }
    impl RecordingDrain {
        fn new() -> Rc<Self> {
            Rc::new(Self {
                sent: RefCell::new(Vec::new()),
                closed: Cell::new(false),
            })
        }
    }
    impl EventDrain for RecordingDrain {
        fn send(&self, timestamp: Time, ent: Time, physical: bool) {
            self.sent.borrow_mut().push((timestamp, ent, physical));
        }
        fn close(&self) {
            self.closed.set(true);
        }
    }

    #[test]
    fn send_events_fires_once_per_instant() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        let drain = RecordingDrain::new();
        ctx.event_drains.borrow_mut().push(drain.clone());
        let source = FixedSource::new(1 * S, true);
        ctx.event_sources.borrow_mut().push(source.clone());

        ctx.try_advance();
        assert!(drain.sent.borrow().is_empty()); // nothing sent before the first advance
        assert_eq!(ctx.now(), 1 * S);

        source.ent.set(Time::FOREVER); // the woken task consumed its event
        source.ready.set(None);
        ctx.try_advance();
        assert_eq!(*drain.sent.borrow(), vec![(1 * S, Time::FOREVER, false)]);

        ctx.try_advance();
        assert_eq!(drain.sent.borrow().len(), 1); // deduped until the next advance
    }

    #[test]
    fn shutting_down_closes_each_drain() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        let drain = RecordingDrain::new();
        ctx.event_drains.borrow_mut().push(drain.clone());
        ctx.shut_down();
        assert!(drain.closed.get());
    }

    #[test]
    fn an_elapsed_timer_is_delivered_before_later_inputs() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        let timer = Rc::new(Timer::new());
        let armed = timer.arm(700 * MS, Instant::now(), false);
        ctx.event_sources.borrow_mut().push(timer.clone());
        ctx.event_sources
            .borrow_mut()
            .push(FixedSource::new(1 * S, true));
        ctx.event_sources
            .borrow_mut()
            .push(FixedSource::new(2 * S, false));

        ctx.try_advance();
        assert_eq!(ctx.now(), 700 * MS);
        assert!(!ctx.physical.get()); // timers are logical

        drop(armed); // the waiting task woke and returned from wait()
        ctx.try_advance();
        assert_eq!(ctx.now(), 1 * S);
    }

    #[test]
    #[should_panic(expected = "ENT would be less than timestamp")]
    fn an_unconsumed_event_at_the_idle_point_is_a_scheduling_bug() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        let timer = Rc::new(Timer::new());
        let _unconsumed = timer.arm(700 * MS, Instant::now(), false);
        ctx.event_sources.borrow_mut().push(timer.clone());

        ctx.try_advance();
        assert_eq!(ctx.now(), 700 * MS);
        // The waiting task still holds its entry, yet the node claims to be idle: the
        // tag's frame would promise 700ms while completing 700ms.
        ctx.try_advance();
    }

    #[test]
    fn concurrent_waits_deliver_in_tag_order() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        let timer = Rc::new(Timer::new());
        let first = timer.arm(700 * MS, Instant::now(), false);
        let second = timer.arm(1 * S, Instant::now(), false);
        ctx.event_sources.borrow_mut().push(timer.clone());
        ctx.event_sources
            .borrow_mut()
            .push(FixedSource::new(2 * S, false));

        ctx.try_advance();
        assert_eq!(ctx.now(), 700 * MS);

        drop(first);
        ctx.try_advance();
        assert_eq!(ctx.now(), 1 * S);

        drop(second);
        ctx.try_advance();
        assert_eq!(ctx.now(), 1 * S); // nothing left to deliver
    }

    #[test]
    fn try_advance_without_a_ready_head_is_a_no_op() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        ctx.try_advance();
        assert_eq!(ctx.now(), Time::ZERO); // nothing registered

        ctx.event_sources
            .borrow_mut()
            .push(FixedSource::new(5 * S, false)); // a promise only
        ctx.try_advance();
        assert_eq!(ctx.now(), Time::ZERO);
    }

    #[test]
    fn a_physical_tag_behind_the_node_is_the_tag_right_after_now() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        ctx.advance_to(5 * S); // the wall clock is still near 0
        assert_eq!(ctx.physical_tag(), (5 * S).next_microstep());
    }

    #[test]
    fn physical_now_runs_on_the_wall_clock() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        ctx.start_time.set(Instant::now());
        let first = ctx.physical_now();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let second = ctx.physical_now();
        assert!(second >= first + 2 * MS, "{second} < {first} + 2ms");
        assert_eq!(ctx.now(), Time::ZERO); // logical time is unaffected
    }

    struct Flag(AtomicBool);
    impl Wake for Flag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn a_timer_wakes_its_waiter_at_its_own_tag() {
        let timer = Timer::new();
        let entry = timer.arm(1 * S, Instant::now(), false);
        let flag = Arc::new(Flag(AtomicBool::new(false)));
        *entry.waker.borrow_mut() = Some(Waker::from(flag.clone()));

        timer.wake_wakers(500 * MS);
        assert!(!flag.0.load(Ordering::SeqCst)); // not due yet

        timer.wake_wakers(1 * S);
        assert!(flag.0.load(Ordering::SeqCst)); // reaching the tag wakes the waiter
    }

    #[test]
    fn a_physical_source_is_delivered_but_never_promised() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        let drain = RecordingDrain::new();
        ctx.event_drains.borrow_mut().push(drain.clone());
        let action = FixedSource::physical(3 * S);
        ctx.event_sources.borrow_mut().push(action.clone());
        ctx.event_sources
            .borrow_mut()
            .push(FixedSource::new(5 * S, false)); // an upstream's promise
        assert_eq!(ctx.ent(), 5 * S); // the physical event is not in the promise

        ctx.try_advance();
        assert_eq!(ctx.now(), 3 * S); // but it is delivered

        action.ready.set(None); // consumed
        ctx.try_advance();
        assert_eq!(*drain.sent.borrow(), vec![(3 * S, 5 * S, true)]); // marked physical
    }

    #[test]
    fn a_timer_armed_at_a_physical_tag_is_physical() {
        let timer = Timer::new();
        let later = timer.arm(1 * S, Instant::now() + Duration::from_secs(3600), true);
        assert_eq!(timer.ent(), Time::FOREVER); // never promised
        assert_eq!(timer.ready(), None); // not due yet
        drop(later);

        let _due = timer.arm(1 * S, Instant::now(), true);
        assert_eq!(timer.ready(), Some(1 * S)); // still delivered
        assert!(timer.wake_wakers(1 * S)); // and its tag is physical

        let _logical = timer.arm(2 * S, Instant::now(), false);
        assert_eq!(timer.ent(), 2 * S);
    }

    /// A node with one timer, its watcher and a recording drain, started now.
    fn node_rig() -> (Rc<Runtime>, Rc<NodeContext>, Rc<Timer>, Rc<RecordingDrain>) {
        let runtime = Rc::new(Runtime::new());
        let ctx = Rc::new(NodeContext::new(runtime.clone()));
        let drain = RecordingDrain::new();
        ctx.event_drains.borrow_mut().push(drain.clone());
        let timer = Rc::new(Timer::new());
        ctx.event_sources.borrow_mut().push(timer.clone());
        runtime.spawn_internal(Timer::watch_timers(timer.clone()));
        ctx.start_time.set(Instant::now());
        (runtime, ctx, timer, drain)
    }

    fn input(ctx: &Rc<NodeContext>) -> (InputPort<i64>, Rc<InputChannel<i64>>) {
        let channel = Rc::new(InputChannel::<i64>::new());
        ctx.event_sources.borrow_mut().push(channel.clone());
        let port = InputPort {
            id: PortId {
                node_id: 0,
                port_id: 0,
            },
            channel: channel.clone(),
            node_context: ctx.clone(),
        };
        (port, channel)
    }

    /// Deliver a value at 2ms, then `wait(1ms)` from that tag. Reports the
    /// physical flag when the wait was armed and when it elapsed, plus the
    /// frames the node sent.
    fn wait_after(physical: bool) -> ((bool, bool), Vec<(Time, Time, bool)>) {
        let (_runtime, ctx, timer, drain) = node_rig();
        let (port, channel) = input(&ctx);
        let node = Node {
            context: ctx.clone(),
            timer,
        };
        if physical {
            channel.push_physical(2 * MS, 7);
        } else {
            channel.push(2 * MS, 7);
        }
        channel.update_remote_ent(Time::FOREVER);

        let seen = Rc::new(Cell::new(None));
        let (out, task_ctx) = (seen.clone(), ctx.clone());
        ctx.run_user(async move {
            port.wait().await;
            let armed_at = task_ctx.physical.get();
            node.wait(1 * MS).await;
            out.set(Some((armed_at, task_ctx.physical.get())));
        });
        (seen.get().unwrap(), drain.sent.borrow().clone())
    }

    #[test]
    fn a_wait_armed_at_a_physical_tag_elapses_physically() {
        let (flags, frames) = wait_after(true);
        assert_eq!(flags, (true, true));
        // The timer is never promised and elapses physically: both frames say so.
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
        let (_runtime, ctx, timer, drain) = node_rig();
        let node = Node {
            context: ctx.clone(),
            timer,
        };
        let (result, worker) = send_later(7);

        let seen = Rc::new(Cell::new(None));
        let (out, task_ctx) = (seen.clone(), ctx.clone());
        ctx.run_user(async move {
            let started = node.wait(1 * MS).await; // a logical tag first
            let value = result.await.unwrap(); // a plain await of a foreign future
            out.set(Some((
                started,
                value,
                task_ctx.now(),
                task_ctx.physical.get(),
            )));
        });
        worker.join().unwrap();

        let (started, value, resumed, physical) = seen.get().unwrap();
        assert_eq!((started, value, physical), (1 * MS, 7, true));
        assert!(resumed > started);
        // Waiting for the result promised nothing; the resumption is physical.
        assert_eq!(
            *drain.sent.borrow(),
            vec![
                (1 * MS, Time::FOREVER, false),
                (resumed, Time::FOREVER, true)
            ]
        );
    }

    #[test]
    fn a_pending_foreign_future_does_not_hold_time() {
        let (runtime, ctx, timer, drain) = node_rig();
        let ticker = Node {
            context: ctx.clone(),
            timer,
        };
        let (done, result) = oneshot::channel::<i64>();

        let seen = Rc::new(Cell::new(None));
        let (out, task_ctx) = (seen.clone(), ctx.clone());
        runtime.spawn(async move {
            let value = result.await.unwrap();
            out.set(Some((task_ctx.now(), task_ctx.physical.get(), value)));
        });
        ctx.run_user(async move {
            ticker.wait(1 * MS).await; // logical time runs while the result is pending
            ticker.wait(1 * MS).await;
            done.send(7).unwrap(); // completed from another task: still foreign to the waiter
        });

        let (resumed, physical, value) = seen.get().unwrap();
        assert!(resumed > 2 * MS, "{resumed}");
        assert_eq!((physical, value), (true, 7));
        assert_eq!(
            *drain.sent.borrow(),
            vec![
                (1 * MS, 2 * MS, false),
                (2 * MS, Time::FOREVER, false),
                (resumed, Time::FOREVER, true)
            ]
        );
    }

    #[test]
    fn a_logical_event_reaching_a_deferred_task_makes_its_tag_physical() {
        let (runtime, ctx, _timer, drain) = node_rig();
        let (port, channel) = input(&ctx);
        let (result, worker) = send_later(7);
        // Once the result is in, a value arrives at 1ms: the silent upstream's promise
        // held the physical resumption back, so the logical event gets there first.
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
        let (out, task_ctx) = (seen.clone(), ctx.clone());
        ctx.run_user(async move {
            let arrived = match select(port.pin(), result).await {
                Either::Left((value, _)) => *value.unwrap().0,
                Either::Right(_) => unreachable!("the value is delivered first"),
            };
            out.set(Some((arrived, task_ctx.now(), task_ctx.physical.get())));
        });
        worker.join().unwrap();
        later.join().unwrap();

        assert_eq!(seen.get(), Some((5, 1 * MS, true))); // observed a foreign result there
        // The absorbed resumption leaves no tag of its own behind.
        assert_eq!(*drain.sent.borrow(), vec![(1 * MS, Time::FOREVER, true)]);
        assert_eq!(ctx.now(), 1 * MS);
    }

    /// Go back to the run queue once.
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
        let (runtime, ctx, timer, _drain) = node_rig();
        let node = Node {
            context: ctx.clone(),
            timer,
        };
        let tags = Rc::new(RefCell::new(Vec::new()));
        for _ in 0..2 {
            let (tags, ctx) = (tags.clone(), ctx.clone());
            runtime.spawn_internal(async move {
                tags.borrow_mut().push(ctx.now());
                yield_once().await; // re-queued: still runnable, so the tag is not complete
                tags.borrow_mut().push(ctx.now());
            });
        }
        let woke_at = Rc::new(Cell::new(None));
        let out = woke_at.clone();
        ctx.run_user(async move { out.set(Some(node.wait(1 * MS).await)) });
        assert_eq!(*tags.borrow(), vec![Time::ZERO; 4]); // all of it ran before any advance
        assert_eq!(woke_at.get(), Some(1 * MS));
    }

    #[test]
    fn every_due_tag_is_completed_before_the_node_waits() {
        let (_runtime, ctx, _timer, drain) = node_rig();
        let (_port, channel) = input(&ctx);
        channel.push(1 * S, 1); // nobody waits for these: delivering them wakes no task
        channel.push(2 * S, 2);
        channel.update_remote_ent(Time::FOREVER);
        let (result, worker) = send_later(7);
        ctx.run_user(async move {
            result.await.unwrap();
        });
        worker.join().unwrap();

        // Both tags were completed back to back, before the result came in.
        let frames = drain.sent.borrow();
        assert_eq!(
            frames[..2],
            [(1 * S, 2 * S, false), (2 * S, Time::FOREVER, false)]
        );
        assert!(frames[2].0 > 2 * S, "{}", frames[2].0); // the resumption, afterwards
    }

    #[test]
    fn a_foreign_wake_of_an_internal_task_runs_at_once() {
        let (runtime, ctx, _timer, _drain) = node_rig();
        let (result, worker) = send_later(7);
        let seen = Rc::new(Cell::new(None));
        let (out, task_ctx) = (seen.clone(), ctx.clone());
        let (done, finished) = oneshot::channel();
        runtime.spawn_internal(async move {
            let value = result.await.unwrap();
            out.set(Some((value, task_ctx.now(), task_ctx.physical.get())));
            done.send(()).unwrap();
        });
        ctx.run(async move { finished.await.unwrap() }, Vec::new());
        worker.join().unwrap();
        assert_eq!(seen.get(), Some((7, Time::ZERO, false))); // no tag of its own
    }

    #[test]
    fn a_logical_waker_woken_from_a_thread_is_not_deferred() {
        let (_runtime, ctx, _timer, _drain) = node_rig();
        let seen = Rc::new(Cell::new(None));
        let (out, task_ctx) = (seen.clone(), ctx.clone());
        ctx.run_user(async move {
            let mut handed_out = false;
            std::future::poll_fn(|_| {
                if handed_out {
                    return Poll::Ready(());
                }
                handed_out = true;
                let logical = Runtime::waker(WakeKind::Logical);
                std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_millis(5));
                    logical.wake();
                });
                Poll::Pending
            })
            .await;
            out.set(Some((task_ctx.now(), task_ctx.physical.get())));
        });
        assert_eq!(seen.get(), Some((Time::ZERO, false))); // polled where it was, at once
    }

    #[test]
    #[should_panic(expected = "already completed")]
    fn setting_an_output_at_the_start_tag_is_a_bug() {
        let ctx = Rc::new(NodeContext::new(Rc::new(Runtime::new())));
        let mut builder = Builder::new(0, ctx);
        let out = builder.output::<i64>();
        out.set(1); // tag 0 completes at startup: nothing can be added to it
    }

    #[test]
    fn a_pending_physical_timer_holds_the_node() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        let timer = Rc::new(Timer::new());
        ctx.event_sources.borrow_mut().push(timer.clone());
        let armed = timer.arm(2 * S, Instant::now() + Duration::from_secs(3600), true);
        ctx.event_sources
            .borrow_mut()
            .push(FixedSource::new(5 * S, true));
        assert_eq!(ctx.ent(), 5 * S); // the physical wait is not promised

        ctx.try_advance();
        assert_eq!(ctx.now(), Time::ZERO); // but the node cannot pass it

        drop(armed);
        let _due = timer.arm(2 * S, Instant::now(), true);
        ctx.try_advance();
        assert_eq!(ctx.now(), 2 * S); // delivered at its own tag first
    }

    #[test]
    fn an_unrelated_advance_does_not_absorb_a_deferred_task() {
        let (runtime, ctx, _timer, _drain) = node_rig();
        let (a, a_channel) = input(&ctx);
        let (_b, b_channel) = input(&ctx);
        a_channel.update_remote_ent(Time::FOREVER); // a silent upstream
        let (result, worker) = send_later(7); // completes at ~5ms: deferred to its own tag
        // B's value at 3ms arrives at ~15ms, after the deferral; its tag comes first.
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
        let (out, task_ctx) = (seen.clone(), ctx.clone());
        ctx.run_user(async move {
            let value = match select(a.pin(), result).await {
                Either::Left(_) => unreachable!("A never delivers"),
                Either::Right((value, _)) => value.unwrap(),
            };
            out.set(Some((value, task_ctx.now(), task_ctx.physical.get())));
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
        let (_runtime, ctx, timer, drain) = node_rig();
        let node = Node {
            context: ctx.clone(),
            timer,
        };
        let (done, result) = oneshot::channel::<i64>();
        done.send(7).unwrap(); // complete before anyone awaits it: no wake-up will come
        let seen = Rc::new(Cell::new(None));
        let (out, task_ctx) = (seen.clone(), ctx.clone());
        ctx.run_user(async move {
            node.wait(1 * MS).await;
            let value = node.await_physical(result).await.unwrap();
            out.set(Some((value, task_ctx.now(), task_ctx.physical.get())));
        });
        let (value, at, physical) = seen.get().unwrap();
        assert_eq!((value, physical), (7, true));
        assert!(at > 1 * MS, "{at}");
        assert_eq!(
            *drain.sent.borrow(),
            vec![(1 * MS, Time::FOREVER, false), (at, Time::FOREVER, true)]
        );
    }

    #[test]
    fn the_clock_starts_at_zero_and_jumps() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        assert_eq!(ctx.now(), Time::ZERO);
        ctx.advance_to(Time::new(10));
        assert_eq!(ctx.now(), Time::new(10));
    }

    #[test]
    #[should_panic(expected = "cannot advance")]
    fn the_clock_is_monotonic() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        ctx.advance_to(Time::new(10));
        ctx.advance_to(Time::new(3));
    }

    #[test]
    #[should_panic(expected = "cannot advance")]
    fn the_clock_rejects_the_current_tag() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        ctx.advance_to(Time::new(10));
        ctx.advance_to(Time::new(10)); // strict: re-announcing `now` is a bug
    }
}
