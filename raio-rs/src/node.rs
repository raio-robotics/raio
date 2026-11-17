use std::cell::RefCell;
use std::rc::Rc;

use crate::runtime::Runtime;
use crate::time::Time;

pub type NodeId = usize;

pub trait EventSource {
    /// Earliest Next Timestamp: the timestamp of the earliest next event that this source will produce.
    fn ent(&self) -> Time;

    /// Whether the earliest next event is ready to be consumed.
    fn ready(&self) -> bool;

    /// Execute events with timestamp <= time. Trigger wakers registered in this source.
    fn wake_wakers(&self, time: Time);
}

pub struct NodeContext {
    runtime: Rc<Runtime>,
    sources: RefCell<Vec<Rc<dyn EventSource>>>,
}

impl NodeContext {
    pub fn new(runtime: Rc<Runtime>) -> Self {
        Self {
            runtime,
            sources: RefCell::new(Vec::new()),
        }
    }

    pub fn now(&self) -> Time {
        self.runtime.now()
    }

    /// It will process any events with timestamp <= time.
    pub fn advance_to(&self, time: Time) {
        self.runtime.advance_to(time);
        for source in self.sources.borrow().iter() {
            source.wake_wakers(time);
        }
    }

    pub fn register_source(&self, source: Rc<dyn EventSource>) {
        self.sources.borrow_mut().push(source);
    }

    /// Earliest Next Timestamp: the timestamp of the earliest next event that this node will produce.
    pub fn ent(&self) -> Time {
        self.sources
            .borrow()
            .iter()
            .map(|source| source.ent())
            .min()
            .unwrap_or(Time::FOREVER)
    }

    pub fn try_advance(&self) {
        // The earliest timestamp of all ready sources.
        let ready_target_time = self
            .sources
            .borrow()
            .iter()
            .filter(|source| source.ready())
            .map(|source| source.ent())
            .min()
            .unwrap_or(Time::NEVER);

        // The earliest timestamp of all unready sources.
        let stat = self
            .sources
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
    fn ent_is_the_min_over_all_sources() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        assert_eq!(ctx.ent(), Time::FOREVER);
        ctx.register_source(FixedSource::new(5 * S, true));
        ctx.register_source(FixedSource::new(2 * S, false)); // possibilities count too
        assert_eq!(ctx.ent(), 2 * S);
    }

    #[test]
    fn try_advance_delivers_the_earliest_consumable() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        ctx.register_source(FixedSource::new(1 * S, true)); // hello's queued frame
        ctx.register_source(FixedSource::new(2 * S, false)); // its piggybacked promise

        ctx.try_advance();
        assert_eq!(ctx.now(), 1 * S); // 1s < promise 2s: delivered
    }

    #[test]
    fn try_advance_blocks_on_an_equal_possibility() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        ctx.register_source(FixedSource::new(2 * S, true)); // left's queued tick
        let announced = FixedSource::new(2 * S, false); // right announced ent=2s
        ctx.register_source(announced.clone());

        ctx.try_advance();
        assert_eq!(ctx.now(), Time::ZERO); // right's 2s value may be in flight

        announced.ready.set(true); // it arrived: the simultaneous tick
        ctx.try_advance();
        assert_eq!(ctx.now(), 2 * S); // both delivered at one instant
    }

    #[test]
    fn closed_upstream_does_not_veto() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        ctx.register_source(FixedSource::new(Time::FOREVER, false)); // closed, drained
        ctx.register_source(FixedSource::new(3 * S, true)); // sibling's queued value

        ctx.try_advance();
        assert_eq!(ctx.now(), 3 * S); // FOREVER promise gates nothing
    }

    #[test]
    fn try_advance_ignores_stale_and_absent_heads() {
        let ctx = NodeContext::new(Rc::new(Runtime::new()));
        ctx.try_advance();
        assert_eq!(ctx.now(), Time::ZERO); // nothing consumable: no-op

        let source = FixedSource::new(5 * MS, true);
        ctx.register_source(source.clone());
        ctx.advance_to(1 * S);
        ctx.try_advance();
        assert_eq!(ctx.now(), 1 * S); // stale head: fold territory, not delivery
    }
}
