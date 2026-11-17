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
            .min(self.stat())
            .max(self.now())
    }

    pub fn update_ents(&self, upstream_id: NodeId, ent: Time) {
        self.ents.borrow_mut().insert(upstream_id, ent);
        self.runtime.wake_wakers();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::{MS, S};
    use std::cell::Cell;
    use std::task::Poll;

    struct FixedSource(Cell<Time>);
    impl EventSource for FixedSource {
        fn ent(&self) -> Time {
            self.0.get()
        }
    }

    fn context(upstreams: &[(NodeId, Time)]) -> NodeContext {
        NodeContext::new(Rc::new(Runtime::new()), upstreams.iter().copied().collect())
    }

    #[test]
    fn no_upstreams_never_block() {
        assert_eq!(context(&[]).stat(), Time::FOREVER);
    }

    #[test]
    fn silent_upstreams_pin_stat_to_their_delay() {
        let ctx = context(&[(0, 10 * MS), (1, 1 * S)]);
        assert_eq!(ctx.stat(), 10 * MS); // unknown ENTs default to ZERO
    }

    #[test]
    fn stat_follows_the_binding_upstream() {
        let ctx = context(&[(0, 10 * MS), (1, 1 * S)]);
        ctx.update_ents(0, 5 * S);
        ctx.update_ents(1, 2 * S);
        assert_eq!(ctx.stat(), 3 * S); // min(5s + 10ms, 2s + 1s)
    }

    #[test]
    fn updates_overwrite_even_downward() {
        let ctx = context(&[(0, Time::ZERO)]);
        ctx.update_ents(0, Time::FOREVER);
        assert_eq!(ctx.stat(), Time::FOREVER);
        ctx.update_ents(0, 7 * S); // a new event reached the upstream
        assert_eq!(ctx.stat(), 7 * S);
    }

    #[test]
    fn idle_node_relays_upstream_bounds() {
        let ctx = context(&[(0, 10 * MS)]);
        ctx.update_ents(0, 1 * S);
        assert_eq!(ctx.ent(), 1 * S + 10 * MS); // no sources: the fold speaks

        let source = Rc::new(FixedSource(Cell::new(500 * MS)));
        ctx.register_source(source.clone());
        assert_eq!(ctx.ent(), 500 * MS); // own pending work is earlier

        source.0.set(Time::FOREVER); // consumed: nothing pending again
        assert_eq!(ctx.ent(), 1 * S + 10 * MS);
    }

    #[test]
    fn ent_never_precedes_now() {
        let ctx = context(&[]);
        ctx.register_source(Rc::new(FixedSource(Cell::new(5 * MS))));
        ctx.runtime.advance_to(1 * S);
        assert_eq!(ctx.ent(), 1 * S); // stale head bumped to the clock
    }

    #[test]
    fn update_wakes_a_parked_future() {
        let runtime = Rc::new(Runtime::new());
        let ctx = Rc::new(NodeContext::new(runtime.clone(), [(0, Time::ZERO)].into()));
        let observed = Rc::new(Cell::new(Time::ZERO));

        // Condvar discipline: check, park, re-check when woken.
        let (c, seen) = (ctx.clone(), observed.clone());
        runtime.spawn(async move {
            std::future::poll_fn(|cx| {
                let stat = c.stat();
                if stat > 1 * S {
                    seen.set(stat);
                    Poll::Ready(())
                } else {
                    c.runtime.register_waker(cx.waker().clone());
                    Poll::Pending
                }
            })
            .await
        });

        let c = ctx.clone();
        runtime.block_on(async move {
            c.update_ents(0, 2 * S); // wakes the parked poll_fn
        });
        runtime.block_on(async {}); // re-enter the executor to run it
        assert_eq!(observed.get(), 2 * S);
    }
}
