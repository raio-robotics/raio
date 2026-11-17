use crate::node::NodeContext;
use crate::node::{EventSource, NodeId};
use crate::time::Time;
use std::any::Any;
use std::cell::Cell;
use std::cell::Ref;
use std::cell::RefCell;
use std::collections::HashMap;
use std::option::Option;
use std::rc::Rc;
use std::task::Poll;
use std::task::Waker;

use crate::action::Action;
use crate::app::BoxFuture;
use crate::node::EventDrain;
use crate::node::{Node, Timer};
use crate::time::NS;
use futures::StreamExt;
use futures::channel::mpsc::unbounded;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct PortId {
    pub node_id: NodeId,
    pub port_id: usize,
}

pub(crate) type InputQueue<T> = RefCell<BTreeMap<Time, (T, bool)>>;

pub(crate) struct InputChannel<T> {
    queue: InputQueue<T>,
    latest_value: RefCell<Option<T>>,
    latest_timestamp: Cell<Time>,
    wakers: RefCell<Vec<(Time, Waker)>>,
    remote_ent: Cell<Time>,
    closed: Cell<bool>,
    delay: Time,
}

impl<T> InputChannel<T> {
    pub fn new(delay: Time) -> Self {
        Self {
            queue: RefCell::new(BTreeMap::new()),
            latest_value: RefCell::new(None),
            latest_timestamp: Cell::new(Time::NEVER),
            wakers: RefCell::new(Vec::new()),
            remote_ent: Cell::new(Time::ZERO),
            closed: Cell::new(false),
            delay,
        }
    }

    pub fn register_waker(&self, timestamp: Time, waker: Waker) {
        self.wakers.borrow_mut().push((timestamp, waker));
    }

    pub(crate) fn push(&self, timestamp: Time, value: T) {
        assert!(
            timestamp >= self.remote_ent.get(),
            "InputChannel broke its ENT promise: {timestamp} arrived after promising {}",
            self.remote_ent.get()
        );
        let evicted = self.queue.borrow_mut().insert(timestamp, (value, false));
        if let Some((value, physical)) = evicted {
            assert!(physical, "InputChannel pushed a logical value twice");
            self.push_physical(timestamp + NS, value);
        }
    }

    pub(crate) fn push_physical(&self, mut timestamp: Time, value: T) {
        let mut queue = self.queue.borrow_mut();
        while queue.contains_key(&timestamp) {
            timestamp = timestamp + NS;
        }
        queue.insert(timestamp, (value, true));
    }

    pub(crate) fn update_remote_ent(&self, ent: Time) {
        let promise = ent + self.delay;
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
        self.wake(Time::FOREVER); // waiters must learn that answer should be None
    }

    fn wake(&self, advance_to: Time) {
        self.wakers.borrow_mut().retain(|(tag, waker)| {
            if *tag <= advance_to {
                waker.wake_by_ref();
                false
            } else {
                true
            }
        });
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

    /// The queue head. A physical head is in hand only once the upstream's promise has passed it.
    fn ready(&self) -> Option<Time> {
        let queue = self.queue.borrow();
        let (head, (_, physical)) = queue.first_key_value()?;
        (!physical || *head < self.remote_ent.get()).then_some(*head)
    }

    fn wake_wakers(&self, time: Time) -> bool {
        let mut physical = false;
        {
            let mut queue = self.queue.borrow_mut();
            let mut latest = self.latest_value.borrow_mut();
            while queue.first_key_value().is_some_and(|(tag, _)| *tag <= time) {
                let (timestamp, (value, is_physical)) = queue.pop_first().unwrap();
                self.latest_timestamp.set(timestamp);
                *latest = Some(value);
                physical |= is_physical;
            }
        }
        self.wake(time);
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
        let timestamp = self.channel.latest_timestamp.get();
        Ref::filter_map(self.channel.latest_value.borrow(), |latest| latest.as_ref())
            .ok()
            .map(|value| (value, timestamp))
    }

    pub fn wait(&self) -> impl Future<Output = Option<(Ref<'_, T>, Time)>> {
        let called_at = self.node_context.now();
        assert!(
            self.channel.latest_timestamp.get() <= called_at,
            "latest timestamp is in the future"
        );
        std::future::poll_fn(move |cx| {
            if self.channel.latest_timestamp.get() > called_at {
                return Poll::Ready(self.latest());
            };
            if self.channel.queue.borrow().is_empty() && self.channel.closed.get() {
                return Poll::Ready(None);
            };
            self.channel.register_waker(called_at, cx.waker().clone());
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

    pub(crate) fn install_sender(&self, sender: OutputSender) {
        self.senders.borrow_mut().push(sender);
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
}

impl<T> OutputPort<T> {
    pub fn set(&self, value: T) {
        *self.channel.cell.borrow_mut() = Some(value);
    }

    pub fn reset(&self) {
        *self.channel.cell.borrow_mut() = None;
    }
}

pub struct Builder {
    node_id: NodeId,
    next_port_id: usize,
    context: Rc<NodeContext>,
    pub(crate) ports: HashMap<PortId, Rc<dyn Any>>,
    pub(crate) action_tasks: Vec<BoxFuture<()>>,
}

impl Builder {
    pub(crate) fn new(node_id: NodeId, context: Rc<NodeContext>) -> Self {
        Self {
            node_id,
            next_port_id: 0,
            context,
            ports: HashMap::new(),
            action_tasks: Vec::new(),
        }
    }

    pub(crate) fn port_count(&self) -> usize {
        self.next_port_id
    }

    pub fn input<T: 'static>(&mut self) -> InputPort<T> {
        let id = self.next_port_id();
        let channel = Rc::new(InputChannel::new(Time::ZERO));
        self.context.register_event_source(channel.clone());
        self.ports.insert(id, channel.clone() as Rc<dyn Any>);
        InputPort {
            id,
            channel,
            node_context: self.context.clone(),
        }
    }

    pub fn action<T: Send + 'static>(&mut self) -> Action<T> {
        let id = self.next_port_id();
        let channel = Rc::new(InputChannel::new(Time::ZERO));
        channel.update_remote_ent(Time::FOREVER); // every value is physical: nothing to promise
        self.context.register_event_source(channel.clone());
        let (sender, mut receiver) = unbounded::<T>();
        let (action_channel, context) = (channel.clone(), self.context.clone());
        self.action_tasks.push(Box::pin(async move {
            while let Some(value) = receiver.next().await {
                action_channel.push_physical(context.physical_tag(Time::ZERO), value);
            }
            action_channel.close(); // the last handle was dropped
        }));
        Action {
            port: InputPort {
                id,
                channel,
                node_context: self.context.clone(),
            },
            initial: RefCell::new(Some(Arc::new(sender))),
            live: RefCell::new(Weak::new()),
        }
    }

    pub fn node(&mut self) -> Node {
        let timer = Rc::new(Timer::new());
        self.context.register_event_source(timer.clone());
        Node {
            context: self.context.clone(),
            timer,
        }
    }

    pub fn output<T: 'static>(&mut self) -> OutputPort<T> {
        let id = self.next_port_id();
        let channel = Rc::new(OutputChannel::new());
        self.context.register_event_drain(channel.clone());
        self.ports.insert(id, channel.clone() as Rc<dyn Any>);
        OutputPort { id, channel }
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
    use crate::time::{MS, S};

    fn channel() -> InputChannel<i64> {
        InputChannel::new(10 * MS)
    }

    #[test]
    fn ent_folds_head_and_remote_bound() {
        let ch = channel();
        assert_eq!(ch.ent(), Time::ZERO); // silent upstream: conservative bound

        ch.update_remote_ent(1 * S); // bound = 1s + 10ms delay
        assert_eq!(ch.ent(), 1 * S + 10 * MS);

        ch.push(2 * S, 42);
        assert_eq!(ch.ent(), 1 * S + 10 * MS); // an earlier arrival is still possible

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
        assert_eq!(*ch.latest_value.borrow(), Some(2)); // newest <= 2s wins
        assert_eq!(ch.latest_timestamp.get(), 2 * S);
        assert_eq!(ch.queue.borrow().len(), 1); // 3s still pending

        ch.wake_wakers(2 * S); // idempotent at the same time
        assert_eq!(ch.latest_timestamp.get(), 2 * S);
    }

    #[test]
    #[should_panic(expected = "broke its ENT promise")]
    fn a_push_before_the_promise_is_an_edge_bug() {
        let ch = channel();
        ch.update_remote_ent(2 * S);
        ch.push(1 * S, 2);
    }

    #[test]
    fn physical_push_lands_in_timestamp_order() {
        let ch = channel();
        ch.push(5 * S, 50);
        ch.push_physical(3 * S, 30); // restamped physical value, earlier than the queued head

        ch.wake_wakers(3 * S);
        assert_eq!(*ch.latest_value.borrow(), Some(30));
        assert_eq!(ch.latest_timestamp.get(), 3 * S);
        assert_eq!(ch.queue.borrow().len(), 1); // 5s still pending
    }

    #[test]
    fn physical_push_skips_taken_tags() {
        let ch = channel();
        ch.push(3 * S, 1);
        ch.push_physical(3 * S, 2);

        ch.wake_wakers(3 * S);
        assert_eq!(*ch.latest_value.borrow(), Some(1)); // the logical value keeps its tag
        ch.wake_wakers(3 * S + NS);
        assert_eq!(*ch.latest_value.borrow(), Some(2));
    }

    #[test]
    fn logical_push_evicts_a_physical_occupant() {
        let ch = channel();
        ch.push_physical(3 * S, 2);
        ch.push(3 * S, 1);

        ch.wake_wakers(3 * S);
        assert_eq!(*ch.latest_value.borrow(), Some(1));
        ch.wake_wakers(3 * S + NS);
        assert_eq!(*ch.latest_value.borrow(), Some(2)); // moved 1ns later
    }

    #[test]
    fn a_logical_head_is_ready_regardless_of_the_promise() {
        let ch = channel();
        ch.push(1 * S, 1); // arrived in order: nothing logical can land before it
        assert_eq!(ch.ready(), Some(1 * S));
    }

    #[test]
    fn a_physical_head_waits_for_the_logical_promise() {
        let ch = channel();
        ch.push_physical(3 * S, 7);
        ch.update_remote_ent(2 * S); // a logical value at 2s + 10ms may still land first
        assert_eq!(ch.ready(), None);
        assert_eq!(ch.ent(), 2 * S + 10 * MS);

        ch.update_remote_ent(4 * S); // the promise passed 3s: the head is provably next
        assert_eq!(ch.ready(), Some(3 * S));
        assert_eq!(ch.ent(), 4 * S + 10 * MS); // a physical head is never promised
    }

    #[test]
    fn ent_promises_only_logical_events() {
        let ch = channel();
        ch.update_remote_ent(5 * S);
        ch.push(7 * S, 70);
        ch.update_remote_ent(10 * S);
        ch.push_physical(3 * S, 30);

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
        assert_eq!(ch.ent(), Time::FOREVER); // saturating +: delay can't wrap it
    }

    #[test]
    fn output_channel_fans_out_to_every_sender() {
        let ch = OutputChannel::<i64>::new();
        let log = Rc::new(RefCell::new(Vec::new()));
        for edge in [1, 2] {
            let log = log.clone();
            ch.install_sender(Box::new(move |timestamp, ent, physical| {
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
            let channel = Rc::new(InputChannel::new(Time::ZERO));
            self.ctx.register_event_source(channel.clone());
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
            self.runtime
                .block_on_with_idle(async {}, || self.ctx.try_advance());
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
        assert!(!done.get()); // parked: a physical value may still arrive

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
        assert!(rig.ctx.physical());

        channel.push(5 * S, 1);
        channel.update_remote_ent(6 * S);
        rig.ctx.try_advance();
        assert_eq!(rig.ctx.now(), 5 * S);
        assert!(!rig.ctx.physical()); // physical mode lasts one tag
    }
}
