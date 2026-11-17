use std::cell::RefCell;
use std::rc::Rc;

use crate::runtime::Runtime;
use crate::time::Time;
use std::cell::Cell;

pub type NodeId = usize;
use std::task::Poll;

use std::task::Waker;
use std::time::Instant;

pub trait EventSource {
    /// Earliest Next Timestamp: the timestamp of the earliest next event that this source will produce.
    fn ent(&self) -> Time;

    /// Whether the earliest next event is ready to be consumed.
    fn ready(&self) -> bool;

    /// Execute events with timestamp <= time. Trigger wakers registered in this source.
    fn wake_wakers(&self, time: Time);
}

pub trait EventDrain {
    fn send(&self, timestamp: Time, ent: Time);
    fn close(&self);
}

pub struct Timer {
    pub(crate) target: Cell<Time>,
    pub(crate) elapsed: Cell<bool>,
    pub(crate) wakers: RefCell<Vec<Waker>>,
}

impl Timer {
    pub(crate) fn new() -> Self {
        Self {
            target: Cell::new(Time::FOREVER),
            elapsed: Cell::new(false),
            wakers: RefCell::new(Vec::new()),
        }
    }

    fn register_waker(&self, waker: Waker) {
        self.wakers.borrow_mut().push(waker);
    }
}

impl EventSource for Timer {
    fn ent(&self) -> Time {
        self.target.get()
    }

    fn ready(&self) -> bool {
        self.elapsed.get() && self.target.get() != Time::FOREVER
    }

    fn wake_wakers(&self, time: Time) {
        if time >= self.target.get() {
            self.target.set(Time::FOREVER);
            self.elapsed.set(false);
            for waker in self.wakers.borrow_mut().drain(..) {
                waker.wake();
            }
        }
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

    pub async fn wait(&self, duration: Time) -> Time {
        assert!(duration > Time::ZERO, "wait needs a positive duration");
        let target = self.context.now() + duration;
        self.timer.target.set(target);
        self.context.try_advance();

        let deadline =
            self.context.start_time.get() + self.context.now().duration() + duration.duration();
        async_io::Timer::at(Instant::from(deadline)).await;
        self.timer.elapsed.set(true);

        std::future::poll_fn(|cx| {
            if self.context.now() >= target {
                return Poll::Ready(());
            }
            self.context.try_advance();
            if self.context.now() >= target {
                return Poll::Ready(());
            }
            self.timer.register_waker(cx.waker().clone());
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
}

impl NodeContext {
    pub fn new(runtime: Rc<Runtime>) -> Self {
        Self {
            runtime,
            start_time: Cell::new(Instant::now()),
            event_sources: RefCell::new(Vec::new()),
            event_drains: RefCell::new(Vec::new()),
            event_drained: Cell::new(true),
        }
    }

    pub fn now(&self) -> Time {
        self.runtime.now()
    }

    pub fn update_start_time(&self) {
        self.start_time.set(Instant::now());
    }

    /// It will process any events with timestamp <= time.
    pub fn advance_to(&self, time: Time) {
        self.event_drained.set(false);
        self.runtime.advance_to(time);
        for source in self.event_sources.borrow().iter() {
            source.wake_wakers(time);
        }
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
        for drain in self.event_drains.borrow().iter() {
            drain.send(timestamp, ent);
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

        // The earliest timestamp of all ready event_sources.
        let ready_target_time = self
            .event_sources
            .borrow()
            .iter()
            .filter(|source| source.ready())
            .map(|source| source.ent())
            .min()
            .unwrap_or(Time::NEVER);

        // The earliest timestamp of all unready event_sources.
        let stat = self
            .event_sources
            .borrow()
            .iter()
            .filter(|source| !source.ready())
            .map(|source| source.ent())
            .min()
            .unwrap_or(Time::FOREVER);
        if ready_target_time <= self.now() {
            return;
        }
        if ready_target_time < stat {
            self.advance_to(ready_target_time);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::{MS, S};
    use std::cell::Cell;

    struct FixedSource {
        ent: Cell<Time>,
        ready: Cell<bool>,
    }
    impl FixedSource {
        fn new(ent: Time, ready: bool) -> Rc<Self> {
            Rc::new(Self {
                ent: Cell::new(ent),
                ready: Cell::new(ready),
            })
        }
    }
    impl EventSource for FixedSource {
        fn ent(&self) -> Time {
            self.ent.get()
        }
        fn ready(&self) -> bool {
            self.ready.get()
        }
        fn wake_wakers(&self, _time: Time) {} // nothing to fold, nobody parked
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

        announced.ready.set(true); // it arrived: the simultaneous tick
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
        sent: RefCell<Vec<(Time, Time)>>,
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
        fn send(&self, timestamp: Time, ent: Time) {
            self.sent.borrow_mut().push((timestamp, ent));
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
        ctx.register_event_source(FixedSource::new(1 * S, true));

        ctx.try_advance();
        assert!(drain.sent.borrow().is_empty()); // nothing sent before the first advance
        assert_eq!(ctx.now(), 1 * S);

        ctx.try_advance();
        assert_eq!(*drain.sent.borrow(), vec![(1 * S, 1 * S)]);

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
    fn try_advance_ignores_stale_and_absent_heads() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        ctx.try_advance();
        assert_eq!(ctx.now(), Time::ZERO); // nothing consumable: no-op

        let source = FixedSource::new(5 * MS, true);
        ctx.register_event_source(source.clone());
        ctx.advance_to(1 * S);
        ctx.try_advance();
        assert_eq!(ctx.now(), 1 * S); // stale head: fold territory, not delivery
    }
}
