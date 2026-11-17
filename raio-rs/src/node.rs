use std::cell::RefCell;
use std::rc::{Rc, Weak};

use crate::runtime::Runtime;
use crate::time::{NS, Time};
use std::cell::Cell;

pub type NodeId = usize;
use std::task::Poll;

use std::task::Waker;
use std::time::Instant;

pub trait EventSource {
    /// Earliest Next Timestamp: The earliest tag at which this source can still produce a logical event.
    /// Promises never to produce logical events with timestamp < ent().
    fn ent(&self) -> Time;

    /// Next Ready Timestamp: timestamp of the next ready to be consumed event.
    /// None if next event is unknown. Promises never to produce any events with timestamp < ready().
    fn ready(&self) -> Option<Time>;

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
}

pub(crate) struct TimerEntry {
    target: Time,
    physical: bool, // armed at a physical tag: fires physically
    pub(crate) elapsed: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

impl Timer {
    pub(crate) fn new() -> Self {
        Self {
            entries: RefCell::new(Vec::new()),
        }
    }

    pub(crate) fn arm(&self, target: Time, physical: bool) -> Rc<TimerEntry> {
        let entry = Rc::new(TimerEntry {
            target,
            physical,
            elapsed: Cell::new(false),
            waker: RefCell::new(None),
        });
        let mut entries = self.entries.borrow_mut();
        entries.retain(|entry| entry.strong_count() > 0);
        entries.push(Rc::downgrade(&entry));
        entry
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

    fn ready(&self) -> Option<Time> {
        let live = self.live();
        let min = live.iter().map(|entry| entry.target).min()?;
        live.iter()
            .any(|entry| entry.target == min && entry.elapsed.get())
            .then_some(min)
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
        self.context.physical()
    }

    pub async fn wait(&self, duration: Time) -> Time {
        assert!(duration > Time::ZERO, "wait needs a positive duration");
        let target = self.context.now() + duration;
        let entry = self.timer.arm(target, self.context.physical());

        let deadline = self.context.start_time.get() + target.duration();
        async_io::Timer::at(deadline).await;
        entry.elapsed.set(true);

        std::future::poll_fn(|cx| {
            if self.context.now() >= target {
                return Poll::Ready(());
            }
            *entry.waker.borrow_mut() = Some(cx.waker().clone());
            Poll::Pending
        })
        .await;
        self.context.now()
    }
}

pub struct NodeContext {
    runtime: Rc<Runtime>,
    pub(crate) start_time: Cell<Instant>,
    event_sources: RefCell<Vec<Rc<dyn EventSource>>>,
    event_drains: RefCell<Vec<Rc<dyn EventDrain>>>,
    event_drained: Cell<bool>,
    physical: Cell<bool>,
}

impl NodeContext {
    pub fn new(runtime: Rc<Runtime>) -> Self {
        Self {
            runtime,
            start_time: Cell::new(Instant::now()),
            event_sources: RefCell::new(Vec::new()),
            event_drains: RefCell::new(Vec::new()),
            event_drained: Cell::new(true),
            physical: Cell::new(false),
        }
    }

    pub fn now(&self) -> Time {
        self.runtime.now()
    }

    pub fn physical_now(&self) -> Time {
        Time::new(self.start_time.get().elapsed().as_nanos() as i64)
    }

    /// The tag a physical event arriving now receives: its wall-clock time plus `delay`,
    /// but never in this node's past.
    pub fn physical_tag(&self, delay: Time) -> Time {
        (self.physical_now() + delay).max(self.now() + NS)
    }

    pub fn update_start_time(&self) {
        self.start_time.set(Instant::now());
    }

    /// It will process any events with timestamp <= time.
    pub fn advance_to(&self, time: Time) {
        self.event_drained.set(false);
        self.runtime.advance_to(time);
        let mut physical = false;
        for source in self.event_sources.borrow().iter() {
            physical |= source.wake_wakers(time);
        }
        self.physical.set(physical);
    }

    pub fn physical(&self) -> bool {
        self.physical.get()
    }

    pub fn register_event_source(&self, source: Rc<dyn EventSource>) {
        self.event_sources.borrow_mut().push(source);
    }

    pub fn register_event_drain(&self, drain: Rc<dyn EventDrain>) {
        self.event_drains.borrow_mut().push(drain);
    }

    pub fn send_events(&self, timestamp: Time, ent: Time) {
        if self.event_drained.get() {
            return;
        }
        assert!(
            ent > timestamp,
            "ENT would be less than timestamp: {ent} < {timestamp}"
        );
        for drain in self.event_drains.borrow().iter() {
            drain.send(timestamp, ent, self.physical.get());
        }
        self.event_drained.set(true);
    }

    pub(crate) fn close_event_drains(&self) {
        for drain in self.event_drains.borrow().iter() {
            drain.close();
        }
        self.event_drains.borrow_mut().clear();
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
            // The earliest event some source already has in hand: the candidate next tag.
            let next = sources.iter().filter_map(|source| source.ready()).min();
            // The earliest tag at which a source with nothing in hand could still produce a logical event.
            let stat = sources
                .iter()
                .filter(|source| source.ready().is_none())
                .map(|source| source.ent())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::port::{InputChannel, InputPort, PortId};
    use crate::time::{MS, S};
    use std::cell::Cell;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Wake;

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
            self.physical // nothing to fold, nobody parked
        }
    }

    #[test]
    fn ent_is_the_min_over_all_event_sources() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        assert_eq!(ctx.ent(), Time::FOREVER);
        ctx.register_event_source(FixedSource::new(5 * S, true));
        ctx.register_event_source(FixedSource::new(2 * S, false)); // possibilities count too
        assert_eq!(ctx.ent(), 2 * S);
    }

    #[test]
    fn try_advance_delivers_the_earliest_consumable() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        ctx.register_event_source(FixedSource::new(1 * S, true)); // hello's queued frame
        ctx.register_event_source(FixedSource::new(2 * S, false)); // its piggybacked promise

        ctx.try_advance();
        assert_eq!(ctx.now(), 1 * S); // 1s < promise 2s: delivered
    }

    #[test]
    fn try_advance_blocks_on_an_equal_possibility() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        ctx.register_event_source(FixedSource::new(2 * S, true)); // left's queued tick
        let announced = FixedSource::new(2 * S, false); // right announced ent=2s
        ctx.register_event_source(announced.clone());

        ctx.try_advance();
        assert_eq!(ctx.now(), Time::ZERO); // right's 2s value may be in flight

        announced.ready.set(Some(2 * S)); // it arrived: the simultaneous tick
        ctx.try_advance();
        assert_eq!(ctx.now(), 2 * S); // both delivered at one instant
    }

    #[test]
    fn closed_upstream_does_not_veto() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        ctx.register_event_source(FixedSource::new(Time::FOREVER, false)); // closed, drained
        ctx.register_event_source(FixedSource::new(3 * S, true)); // sibling's queued value

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
        ctx.register_event_drain(drain.clone());
        let source = FixedSource::new(1 * S, true);
        ctx.register_event_source(source.clone());

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
    fn close_event_drains_closes_each_drain() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        let drain = RecordingDrain::new();
        ctx.register_event_drain(drain.clone());
        ctx.close_event_drains();
        assert!(drain.closed.get());
    }

    #[test]
    fn an_elapsed_timer_is_delivered_before_later_inputs() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        let timer = Rc::new(Timer::new());
        let armed = timer.arm(700 * MS, false);
        armed.elapsed.set(true);
        ctx.register_event_source(timer.clone());
        ctx.register_event_source(FixedSource::new(1 * S, true));
        ctx.register_event_source(FixedSource::new(2 * S, false));

        ctx.try_advance();
        assert_eq!(ctx.now(), 700 * MS);
        assert!(!ctx.physical()); // timers are logical

        drop(armed); // the sleeper woke and returned from wait()
        ctx.try_advance();
        assert_eq!(ctx.now(), 1 * S);
    }

    #[test]
    #[should_panic(expected = "ENT would be less than timestamp")]
    fn an_unconsumed_event_at_the_idle_point_is_a_scheduling_bug() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        let timer = Rc::new(Timer::new());
        let armed = timer.arm(700 * MS, false);
        armed.elapsed.set(true);
        ctx.register_event_source(timer.clone());

        ctx.try_advance();
        assert_eq!(ctx.now(), 700 * MS);
        // The sleeper still holds its entry, yet the node claims to be idle: the
        // tag's frame would promise 700ms while completing 700ms.
        ctx.try_advance();
    }

    #[test]
    fn concurrent_waits_deliver_in_tag_order() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        let timer = Rc::new(Timer::new());
        let first = timer.arm(700 * MS, false);
        let second = timer.arm(1 * S, false);
        first.elapsed.set(true);
        second.elapsed.set(true);
        ctx.register_event_source(timer.clone());
        ctx.register_event_source(FixedSource::new(2 * S, false));

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

        ctx.register_event_source(FixedSource::new(5 * S, false)); // a promise only
        ctx.try_advance();
        assert_eq!(ctx.now(), Time::ZERO);
    }

    #[test]
    fn physical_now_runs_on_the_wall_clock() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        ctx.update_start_time();
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
    fn a_timer_wakes_its_sleeper_at_its_own_tag() {
        let timer = Timer::new();
        let entry = timer.arm(1 * S, false);
        let flag = Arc::new(Flag(AtomicBool::new(false)));
        *entry.waker.borrow_mut() = Some(Waker::from(flag.clone()));

        timer.wake_wakers(500 * MS);
        assert!(!flag.0.load(Ordering::SeqCst)); // not due yet

        timer.wake_wakers(1 * S);
        assert!(flag.0.load(Ordering::SeqCst)); // reaching the tag wakes the sleeper
    }

    #[test]
    fn a_physical_source_is_delivered_but_never_promised() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        let drain = RecordingDrain::new();
        ctx.register_event_drain(drain.clone());
        let action = FixedSource::physical(3 * S);
        ctx.register_event_source(action.clone());
        ctx.register_event_source(FixedSource::new(5 * S, false)); // an upstream's promise
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
        let entry = timer.arm(1 * S, true);
        assert_eq!(timer.ent(), Time::FOREVER); // never promised
        assert_eq!(timer.ready(), None); // not elapsed yet

        entry.elapsed.set(true);
        assert_eq!(timer.ready(), Some(1 * S)); // still delivered
        assert!(timer.wake_wakers(1 * S)); // and its tag is physical

        let _logical = timer.arm(2 * S, false);
        assert_eq!(timer.ent(), 2 * S);
    }

    /// Deliver a value at 2ms, then `wait(1ms)` from that tag. Reports the
    /// physical flag when the wait was armed and when it fired, plus the
    /// frames the node sent.
    fn wait_after(physical: bool) -> ((bool, bool), Vec<(Time, Time, bool)>) {
        let runtime = Rc::new(Runtime::new());
        let ctx = Rc::new(NodeContext::new(runtime.clone()));
        let channel = Rc::new(InputChannel::<i64>::new(Time::ZERO));
        ctx.register_event_source(channel.clone());
        let drain = RecordingDrain::new();
        ctx.register_event_drain(drain.clone());
        let timer = Rc::new(Timer::new());
        ctx.register_event_source(timer.clone());
        let node = Node {
            context: ctx.clone(),
            timer,
        };
        let port = InputPort {
            id: PortId {
                node_id: 0,
                port_id: 0,
            },
            channel: channel.clone(),
            node_context: ctx.clone(),
        };
        if physical {
            channel.push_physical(2 * MS, 7);
        } else {
            channel.push(2 * MS, 7);
        }
        channel.update_remote_ent(Time::FOREVER);
        ctx.update_start_time();

        let seen = Rc::new(Cell::new(None));
        let (out, task_ctx, idle_ctx) = (seen.clone(), ctx.clone(), ctx.clone());
        runtime.block_on_with_idle(
            async move {
                port.wait().await;
                let armed_at = task_ctx.physical();
                node.wait(1 * MS).await;
                out.set(Some((armed_at, task_ctx.physical())));
            },
            move || idle_ctx.try_advance(),
        );
        (seen.get().unwrap(), drain.sent.borrow().clone())
    }

    #[test]
    fn a_wait_armed_at_a_physical_tag_fires_physically() {
        let (flags, frames) = wait_after(true);
        assert_eq!(flags, (true, true));
        // The timer is never promised and fires physically: both frames say so.
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
}
