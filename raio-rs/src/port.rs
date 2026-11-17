use std::any::Any;
use std::cell::{Cell, Ref, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Poll, Waker};

use futures::StreamExt;
use futures::channel::mpsc::unbounded;
use serde::{Deserialize, Serialize};

use crate::action::{Action, Handles};
use crate::node::{EventDrain, EventSource, Node, NodeContext, NodeId, Timer};
use crate::runtime::{BoxFuture, Runtime, TaskId, WakeKind};
use crate::time::Time;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct PortId {
    pub node_id: NodeId,
    pub port_id: usize,
}

pub(crate) type InputQueue<T> = RefCell<BTreeMap<Time, (T, bool)>>;

pub(crate) struct InputChannel<T> {
    queue: InputQueue<T>,
    /// The last delivered value and its tag.
    latest: RefCell<Option<(Time, T)>>,
    /// Tasks waiting in `wait`: one waker for a value at its tag, one for a close.
    waiters: RefCell<HashMap<TaskId, (Waker, Waker)>>,
    remote_ent: Cell<Time>,
    closed: Cell<bool>,
}

impl<T> InputChannel<T> {
    pub fn new() -> Self {
        Self {
            queue: RefCell::new(BTreeMap::new()),
            latest: RefCell::new(None),
            waiters: RefCell::new(HashMap::new()),
            remote_ent: Cell::new(Time::ZERO),
            closed: Cell::new(false),
        }
    }

    fn latest_timestamp(&self) -> Time {
        self.latest
            .borrow()
            .as_ref()
            .map_or(Time::NEVER, |(timestamp, _)| *timestamp)
    }

    /// The tag of the last value received, queued or delivered: values are delivered in
    /// the order they were sent.
    fn last_received(&self) -> Time {
        self.queue
            .borrow()
            .last_key_value()
            .map_or_else(|| self.latest_timestamp(), |(tag, _)| *tag)
    }

    pub(crate) fn push(&self, timestamp: Time, value: T) {
        assert!(
            timestamp >= self.remote_ent.get(),
            "edge broke its ENT promise: {timestamp} arrived after promising {}",
            self.remote_ent.get()
        );
        assert!(
            timestamp > self.last_received(),
            "edge delivered a logical value at {timestamp} behind one at {}",
            self.last_received()
        );
        self.queue.borrow_mut().insert(timestamp, (value, false));
    }

    /// Queue a physical value at `tag`, or right after the last value if `tag` is not
    /// after it: physical values take their sender's tag but never overtake.
    pub(crate) fn push_physical(&self, tag: Time, value: T) {
        let tag = tag.max(self.last_received().next_microstep());
        let taken = self.queue.borrow_mut().insert(tag, (value, true));
        assert!(taken.is_none(), "two values at {tag}");
    }

    pub(crate) fn update_remote_ent(&self, promise: Time) {
        assert!(
            promise >= self.remote_ent.get(),
            "edge lowered its ENT promise from {} to {promise}",
            self.remote_ent.get()
        );
        self.remote_ent.set(promise);
    }

    pub(crate) fn close(&self) {
        self.update_remote_ent(Time::FOREVER);
        self.closed.set(true);
        // Queued values wake the waiters at their tags, and `wait` then sees the close.
        // With nothing queued, a close has no logical time: the waiters resume physically.
        if self.queue.borrow().is_empty() {
            for (_, (_, physical)) in self.waiters.borrow_mut().drain() {
                physical.wake();
            }
        }
    }

    fn wake(&self) {
        for (_, (logical, _)) in self.waiters.borrow_mut().drain() {
            logical.wake();
        }
    }
}

impl<T> EventSource for InputChannel<T> {
    fn ent(&self) -> Time {
        // The earliest queued logical value is promised; physical ones ahead of it are not.
        self.queue
            .borrow()
            .iter()
            .find(|(_, (_, physical))| !physical)
            .map_or(Time::FOREVER, |(timestamp, _)| *timestamp)
            .min(self.remote_ent.get())
    }

    /// The queue head. A physical head is delivered only once the upstream's promise has passed it.
    fn ready(&self) -> Option<Time> {
        let queue = self.queue.borrow();
        let (head, (_, physical)) = queue.first_key_value()?;
        (!physical || *head < self.remote_ent.get()).then_some(*head)
    }

    fn wake_wakers(&self, time: Time) -> bool {
        let mut physical = false;
        let mut delivered = false;
        {
            let mut queue = self.queue.borrow_mut();
            while queue.first_key_value().is_some_and(|(tag, _)| *tag <= time) {
                let (timestamp, (value, is_physical)) = queue.pop_first().unwrap();
                *self.latest.borrow_mut() = Some((timestamp, value));
                physical |= is_physical;
                delivered = true;
            }
        }
        if delivered {
            self.wake(); // only a delivery changes what the waiters see
        }
        physical
    }
}

pub struct InputPort<T> {
    pub(crate) id: PortId,
    pub(crate) channel: Rc<InputChannel<T>>,
    pub(crate) node_context: Rc<NodeContext>,
}

impl<T> InputPort<T> {
    /// Read the latest value from the port
    pub fn get(&self) -> Option<Ref<'_, T>> {
        let now = self.node_context.now();
        self.latest()
            .and_then(|(value, timestamp)| (timestamp == now).then_some(value))
    }

    pub fn latest(&self) -> Option<(Ref<'_, T>, Time)> {
        let timestamp = self.channel.latest_timestamp();
        Ref::filter_map(self.channel.latest.borrow(), |latest| {
            latest.as_ref().map(|(_, value)| value)
        })
        .ok()
        .map(|value| (value, timestamp))
    }

    pub fn wait(&self) -> impl Future<Output = Option<(Ref<'_, T>, Time)>> {
        let called_at = self.node_context.now();
        assert!(
            self.channel.latest_timestamp() <= called_at,
            "latest timestamp is in the future"
        );
        std::future::poll_fn(move |_| {
            if self.channel.latest_timestamp() > called_at {
                return Poll::Ready(self.latest());
            };
            if self.channel.queue.borrow().is_empty() && self.channel.closed.get() {
                return Poll::Ready(None);
            };
            self.channel.waiters.borrow_mut().insert(
                Runtime::current_task(),
                (
                    Runtime::waker(WakeKind::Logical),
                    Runtime::waker(WakeKind::Physical),
                ),
            );
            Poll::Pending
        })
    }

    pub fn pin(&self) -> Pin<Box<impl Future<Output = Option<(Ref<'_, T>, Time)>>>> {
        Box::pin(self.wait())
    }

    pub fn trigger(&self) -> Pin<Box<dyn Future<Output = bool> + '_>> {
        let wait = self.wait();
        Box::pin(async move { wait.await.is_some() })
    }
}

pub(crate) type OutputSender = Box<dyn Fn(Time, Time, bool)>;

pub struct OutputChannel<T> {
    pub(crate) cell: RefCell<Option<T>>,
    pub(crate) senders: RefCell<Vec<OutputSender>>,
}

impl<T> OutputChannel<T> {
    fn new() -> Self {
        Self {
            cell: RefCell::new(None),
            senders: RefCell::new(Vec::new()),
        }
    }
}

impl<T> EventDrain for OutputChannel<T> {
    fn send(&self, timestamp: Time, ent: Time, physical: bool) {
        for sender in self.senders.borrow().iter() {
            sender(timestamp, ent, physical);
        }
    }

    fn close(&self) {
        self.senders.borrow_mut().clear();
    }
}

pub struct OutputPort<T> {
    pub(crate) id: PortId,
    pub(crate) channel: Rc<OutputChannel<T>>,
    pub(crate) node_context: Rc<NodeContext>,
}

impl<T> OutputPort<T> {
    pub fn set(&self, value: T) {
        assert!(
            !self.node_context.tag_complete.get(),
            "output set at {}, a tag the node already completed; tag 0 completes at startup, so wait first",
            self.node_context.now()
        );
        *self.channel.cell.borrow_mut() = Some(value);
    }

    pub fn reset(&self) {
        *self.channel.cell.borrow_mut() = None;
    }
}

pub struct Builder {
    node_id: NodeId,
    pub(crate) next_port_id: usize,
    context: Rc<NodeContext>,
    pub(crate) ports: HashMap<PortId, Rc<dyn Any>>,
    /// Every input port: each needs an upstream edge.
    pub(crate) inputs: Vec<PortId>,
    /// raio's own tasks for this node: the watcher of each timer, the receive
    /// loop of each action.
    pub(crate) tasks: Vec<BoxFuture<()>>,
}

impl Builder {
    pub(crate) fn new(node_id: NodeId, context: Rc<NodeContext>) -> Self {
        Self {
            node_id,
            next_port_id: 0,
            context,
            ports: HashMap::new(),
            inputs: Vec::new(),
            tasks: Vec::new(),
        }
    }

    pub fn input<T: 'static>(&mut self) -> InputPort<T> {
        let id = self.next_port_id();
        let channel = Rc::new(InputChannel::new());
        self.context
            .event_sources
            .borrow_mut()
            .push(channel.clone());
        self.ports.insert(id, channel.clone() as Rc<dyn Any>);
        self.inputs.push(id);
        InputPort {
            id,
            channel,
            node_context: self.context.clone(),
        }
    }

    pub fn action<T: Send + 'static>(&mut self) -> Action<T> {
        let id = self.next_port_id();
        let channel = Rc::new(InputChannel::new());
        channel.update_remote_ent(Time::FOREVER); // every value is physical: nothing to promise
        self.context
            .event_sources
            .borrow_mut()
            .push(channel.clone());
        let (sender, mut receiver) = unbounded::<T>();
        let (action_channel, context) = (channel.clone(), self.context.clone());
        self.tasks.push(Box::pin(async move {
            while let Some(value) = receiver.next().await {
                action_channel.push_physical(context.physical_tag(), value);
            }
            action_channel.close(); // the last handle was dropped
        }));
        Action {
            port: InputPort {
                id,
                channel,
                node_context: self.context.clone(),
            },
            handles: RefCell::new(Handles::Unused(Arc::new(sender))),
        }
    }

    pub fn node(&mut self) -> Node {
        let timer = Rc::new(Timer::new());
        self.context.event_sources.borrow_mut().push(timer.clone());
        self.tasks
            .push(Box::pin(Timer::watch_timers(timer.clone())));
        Node {
            context: self.context.clone(),
            timer,
        }
    }

    pub fn output<T: 'static>(&mut self) -> OutputPort<T> {
        let id = self.next_port_id();
        let channel = Rc::new(OutputChannel::new());
        self.context.event_drains.borrow_mut().push(channel.clone());
        self.ports.insert(id, channel.clone() as Rc<dyn Any>);
        OutputPort {
            id,
            channel,
            node_context: self.context.clone(),
        }
    }

    fn next_port_id(&mut self) -> PortId {
        let id = PortId {
            node_id: self.node_id,
            port_id: self.next_port_id,
        };
        self.next_port_id += 1;
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::S;

    fn channel() -> InputChannel<i64> {
        InputChannel::new()
    }

    #[test]
    fn ent_folds_head_and_remote_bound() {
        let ch = channel();
        assert_eq!(ch.ent(), Time::ZERO); // silent upstream: conservative bound

        ch.update_remote_ent(1 * S);
        assert_eq!(ch.ent(), 1 * S);

        ch.push(2 * S, 42);
        assert_eq!(ch.ent(), 1 * S); // an earlier arrival is still possible

        ch.update_remote_ent(5 * S);
        assert_eq!(ch.ent(), 2 * S); // now the queued head is earliest
    }

    #[test]
    fn latch_folds_tags_up_to_time() {
        let ch = channel();
        ch.push(1 * S, 1);
        ch.push(2 * S, 2);
        ch.push(3 * S, 3);

        ch.wake_wakers(2 * S);
        assert_eq!(*ch.latest.borrow(), Some((2 * S, 2))); // newest <= 2s wins
        assert_eq!(ch.queue.borrow().len(), 1); // 3s still pending

        ch.wake_wakers(2 * S); // idempotent at the same time
        assert_eq!(*ch.latest.borrow(), Some((2 * S, 2)));
    }

    #[test]
    #[should_panic(expected = "broke its ENT promise")]
    fn a_push_before_the_promise_is_an_edge_bug() {
        let ch = channel();
        ch.update_remote_ent(2 * S);
        ch.push(1 * S, 2);
    }

    #[test]
    fn a_physical_value_never_overtakes_an_earlier_one() {
        let ch = channel();
        ch.push(5 * S, 50);
        ch.push_physical(3 * S, 30); // sent after the 5s value: delivered after it

        ch.wake_wakers(5 * S);
        assert_eq!(*ch.latest.borrow(), Some((5 * S, 50)));
        ch.wake_wakers((5 * S).next_microstep());
        assert_eq!(*ch.latest.borrow(), Some(((5 * S).next_microstep(), 30)));
    }

    #[test]
    fn a_physical_value_at_a_taken_tag_takes_the_next_microstep() {
        let ch = channel();
        ch.push(3 * S, 1);
        ch.push_physical(3 * S, 2);

        ch.wake_wakers(3 * S);
        assert_eq!(*ch.latest.borrow(), Some((3 * S, 1))); // the logical value keeps its tag
        ch.wake_wakers((3 * S).next_microstep());
        assert_eq!(*ch.latest.borrow(), Some(((3 * S).next_microstep(), 2)));
    }

    #[test]
    #[should_panic(expected = "behind one at")]
    fn a_logical_value_behind_a_physical_one_is_an_edge_bug() {
        let ch = channel();
        ch.push_physical(3 * S, 2);
        ch.push(3 * S, 1); // sender tags only increase: this cannot come from the same edge
    }

    #[test]
    fn a_logical_head_is_ready_regardless_of_the_promise() {
        let ch = channel();
        ch.push(1 * S, 1); // arrived in order: nothing logical can arrive before it
        assert_eq!(ch.ready(), Some(1 * S));
    }

    #[test]
    fn a_physical_head_waits_for_the_logical_promise() {
        let ch = channel();
        ch.push_physical(3 * S, 7);
        ch.update_remote_ent(2 * S); // a logical value at 2s + 10ms may still arrive first
        assert_eq!(ch.ready(), None);
        assert_eq!(ch.ent(), 2 * S);

        ch.update_remote_ent(4 * S); // the promise passed 3s: the head is provably next
        assert_eq!(ch.ready(), Some(3 * S));
        assert_eq!(ch.ent(), 4 * S); // a physical head is never promised
    }

    #[test]
    fn ent_promises_only_logical_events() {
        let ch = channel();
        ch.push_physical(3 * S, 30); // a physical frame, promising 5s
        ch.update_remote_ent(5 * S);
        ch.push(7 * S, 70); // then a logical one, promising 10s
        ch.update_remote_ent(10 * S);

        assert_eq!(ch.ready(), Some(3 * S)); // delivered first
        assert_eq!(ch.ent(), 7 * S); // but only the logical value is promised
    }

    #[test]
    #[should_panic(expected = "lowered its ENT promise")]
    fn a_promise_never_moves_backwards() {
        let ch = channel();
        ch.update_remote_ent(5 * S);
        ch.update_remote_ent(4 * S);
    }

    #[test]
    fn close_is_a_forever_promise() {
        let ch = channel();
        ch.close();
        assert_eq!(ch.ent(), Time::FOREVER);
    }

    #[test]
    fn output_channel_fans_out_to_every_sender() {
        let ch = OutputChannel::<i64>::new();
        let log = Rc::new(RefCell::new(Vec::new()));
        for edge in [1, 2] {
            let log = log.clone();
            ch.senders
                .borrow_mut()
                .push(Box::new(move |timestamp, ent, physical| {
                    log.borrow_mut().push((edge, timestamp, ent, physical));
                }));
        }

        ch.send(1 * S, 2 * S, true);
        assert_eq!(
            *log.borrow(),
            vec![(1, 1 * S, 2 * S, true), (2, 1 * S, 2 * S, true)]
        );

        ch.close();
        ch.send(3 * S, 4 * S, false);
        assert_eq!(log.borrow().len(), 2); // closed: senders dropped
    }

    // --- integration: real futures over runtime + context + wait() ---

    use crate::runtime::Runtime;
    use std::cell::Cell;

    struct Rig {
        runtime: Rc<Runtime>,
        ctx: Rc<NodeContext>,
    }

    impl Rig {
        fn new() -> Self {
            let runtime = Rc::new(Runtime::new());
            let ctx = Rc::new(NodeContext::new(runtime.clone()));
            Self { runtime, ctx }
        }

        fn port(&self, port_id: usize) -> (InputPort<i64>, Rc<InputChannel<i64>>) {
            let channel = Rc::new(InputChannel::new());
            self.ctx.event_sources.borrow_mut().push(channel.clone());
            let port = InputPort {
                id: PortId {
                    node_id: 0,
                    port_id,
                },
                channel: channel.clone(),
                node_context: self.ctx.clone(),
            };
            (port, channel)
        }

        /// Let woken tasks run until everything is parked again, advancing at
        /// each idle point the way a node's runner does.
        fn settle(&self) {
            self.ctx.run_until_idle();
        }
    }

    #[test]
    fn wait_delivers_after_arrival_and_ends_on_close() {
        let rig = Rig::new();
        let (port, channel) = rig.port(0);
        let seen = Rc::new(RefCell::new(Vec::new()));
        let done = Rc::new(Cell::new(false));

        let (out, fin) = (seen.clone(), done.clone());
        rig.runtime.spawn(async move {
            while let Some((value, timestamp)) = port.wait().await {
                out.borrow_mut().push((*value, timestamp));
            }
            fin.set(true);
        });
        rig.settle(); // parks: empty queue, upstream open
        assert!(seen.borrow().is_empty());

        // The receive-task contract: push + ENT; the idle point advances.
        channel.push(1 * S, 42);
        channel.update_remote_ent(2 * S);
        rig.settle();
        assert_eq!(*seen.borrow(), vec![(42, 1 * S)]);
        assert_eq!(rig.ctx.now(), 1 * S);

        channel.close();
        rig.settle();
        assert!(done.get()); // drained + closed = None
    }

    #[test]
    fn a_pending_promise_blocks_until_resolved() {
        let rig = Rig::new();
        let (left, left_ch) = rig.port(0);
        let (_right, right_ch) = rig.port(1);
        let seen = Rc::new(RefCell::new(Vec::new()));

        let out = seen.clone();
        rig.runtime.spawn(async move {
            if let Some((value, _)) = left.wait().await {
                out.borrow_mut().push(*value);
            }
        });
        rig.settle();

        // Left's tick is queued, but right has only announced ent=2s:
        // its 2s value may still be in flight.
        left_ch.push(2 * S, 1);
        left_ch.update_remote_ent(4 * S);
        right_ch.update_remote_ent(2 * S);
        rig.ctx.try_advance();
        rig.settle();
        assert!(seen.borrow().is_empty()); // blocked: 2s == promise
        assert_eq!(rig.ctx.now(), Time::ZERO);

        // Right's ENT moves past 2s: left's tick is provably next.
        right_ch.update_remote_ent(4 * S);
        rig.ctx.try_advance();
        rig.settle();
        assert_eq!(*seen.borrow(), vec![1]);
        assert_eq!(rig.ctx.now(), 2 * S);
    }

    #[test]
    fn simultaneous_tags_settle_in_one_instant() {
        let rig = Rig::new();
        let (left, left_ch) = rig.port(0);
        let (right, right_ch) = rig.port(1);
        let seen = Rc::new(RefCell::new(Vec::new()));

        let out = seen.clone();
        rig.runtime.spawn(async move {
            if let Some((value, _)) = left.wait().await {
                // Same instant: the sibling's simultaneous value is a
                // settled fact, readable without waiting.
                let sibling = right.latest().map(|(v, timestamp)| (*v, timestamp));
                out.borrow_mut().push((*value, sibling));
            }
        });
        rig.settle();

        left_ch.push(2 * S, 1);
        left_ch.update_remote_ent(4 * S);
        right_ch.push(2 * S, 7);
        right_ch.update_remote_ent(4 * S);
        rig.ctx.try_advance();
        rig.settle();

        // One advance latched both queued 2s values.
        assert_eq!(*seen.borrow(), vec![(1, Some((7, 2 * S)))]);
        assert_eq!(rig.ctx.now(), 2 * S);
    }

    #[test]
    fn wait_resolves_via_latest_even_after_further_advances() {
        let rig = Rig::new();
        let (right, right_ch) = rig.port(0);
        let (_left, left_ch) = rig.port(1);
        let seen = Rc::new(RefCell::new(Vec::new()));

        let out = seen.clone();
        rig.runtime.spawn(async move {
            if let Some((value, timestamp)) = right.wait().await {
                out.borrow_mut().push((*value, timestamp));
            }
        });
        rig.settle();

        right_ch.push(3 * S, 30);
        right_ch.update_remote_ent(10 * S);
        left_ch.push(5 * S, 50);
        left_ch.update_remote_ent(10 * S);
        rig.ctx.try_advance();
        rig.ctx.try_advance();
        assert_eq!(rig.ctx.now(), 5 * S);
        rig.settle();

        assert_eq!(*seen.borrow(), vec![(30, 3 * S)]);
    }

    #[test]
    fn wait_baseline_is_at_call_not_first_poll() {
        let rig = Rig::new();
        let (port, channel) = rig.port(0);
        let ctx = rig.ctx.clone();
        let seen = Rc::new(RefCell::new(None));

        let out = seen.clone();
        rig.runtime.spawn(async move {
            let pending = port.wait();
            // Deliver into the gap between creation and first poll —
            // the select() blind spot.
            channel.push(1 * S, 7);
            channel.close();
            ctx.try_advance();
            *out.borrow_mut() = Some(pending.await.map(|(value, timestamp)| (*value, timestamp)));
        });
        rig.settle();

        assert_eq!(*seen.borrow(), Some(Some((7, 1 * S))));
    }

    #[test]
    fn wait_drains_a_closed_queue_before_ending() {
        let rig = Rig::new();
        let (port, channel) = rig.port(0);
        let seen = Rc::new(RefCell::new(Vec::new()));
        let done = Rc::new(Cell::new(false));

        let (out, fin) = (seen.clone(), done.clone());
        rig.runtime.spawn(async move {
            while let Some((value, _)) = port.wait().await {
                out.borrow_mut().push(*value);
            }
            fin.set(true);
        });
        rig.settle();

        channel.push(1 * S, 1);
        channel.push(2 * S, 2);
        channel.close(); // FOREVER promise; two values still queued
        rig.settle();

        // The idle point cascades: 1s, then 2s, then None.
        assert_eq!(*seen.borrow(), vec![1, 2]);
        assert!(done.get());
        assert_eq!(rig.ctx.now(), 2 * S);
    }

    #[test]
    fn a_close_resumes_the_waiter_at_a_physical_tag() {
        let rig = Rig::new();
        let (port, channel) = rig.port(0);
        let seen = Rc::new(Cell::new(None));

        let (out, ctx) = (seen.clone(), rig.ctx.clone());
        rig.runtime.spawn(async move {
            assert!(port.wait().await.is_none());
            out.set(Some((ctx.now(), ctx.physical.get())));
        });
        rig.settle(); // waiting at tag 0

        channel.close(); // no logical time of its own: the waiter gets a fresh physical tag
        rig.settle();
        let (tag, physical) = seen.get().unwrap();
        assert!(tag > Time::ZERO, "{tag}");
        assert!(physical);
    }

    #[test]
    fn a_forever_promise_is_not_a_close() {
        let rig = Rig::new();
        let (port, channel) = rig.port(0);
        let done = Rc::new(Cell::new(false));

        // No more logical values, but the upstream is still connected.
        channel.update_remote_ent(Time::FOREVER);
        let fin = done.clone();
        rig.runtime.spawn(async move {
            assert!(port.wait().await.is_none());
            fin.set(true);
        });
        rig.settle();
        assert!(!done.get()); // waiting: a physical value may still arrive

        channel.close();
        rig.settle();
        assert!(done.get());
    }

    #[test]
    fn a_physical_value_marks_its_tag_and_only_its_tag() {
        let rig = Rig::new();
        let (_port, channel) = rig.port(0);

        channel.push_physical(3 * S, 7);
        channel.update_remote_ent(4 * S);
        rig.ctx.try_advance();
        assert_eq!(rig.ctx.now(), 3 * S);
        assert!(rig.ctx.physical.get());

        channel.push(5 * S, 1);
        channel.update_remote_ent(6 * S);
        rig.ctx.try_advance();
        assert_eq!(rig.ctx.now(), 5 * S);
        assert!(!rig.ctx.physical.get()); // physical mode lasts one tag
    }
}
