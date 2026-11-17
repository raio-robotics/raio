use crate::node::NodeContext;
use crate::node::{EventSource, NodeId};
use crate::time::Time;
use std::any::Any;
use std::cell::Cell;
use std::cell::Ref;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::option::Option;
use std::rc::Rc;
use std::task::Poll;
use std::task::Waker;

use crate::node::EventDrain;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct PortId {
    pub node_id: NodeId,
    pub port_id: usize,
}

pub(crate) type InputQueue<T> = RefCell<VecDeque<(Time, T)>>;

pub(crate) struct InputChannel<T> {
    queue: InputQueue<T>,
    latest_value: RefCell<Option<T>>,
    latest_timestamp: Cell<Time>,
    wakers: RefCell<Vec<(Time, Waker)>>,
    remote_ent: Cell<Time>,
    delay: Time,
}

impl<T> InputChannel<T> {
    pub fn new(delay: Time) -> Self {
        Self {
            queue: RefCell::new(VecDeque::new()),
            latest_value: RefCell::new(None),
            latest_timestamp: Cell::new(Time::NEVER),
            wakers: RefCell::new(Vec::new()),
            remote_ent: Cell::new(Time::ZERO),
            delay,
        }
    }

    pub fn register_waker(&self, timestamp: Time, waker: Waker) {
        self.wakers.borrow_mut().push((timestamp, waker));
    }

    pub(crate) fn push(&self, timestamp: Time, value: T) {
        {
            let mut queue = self.queue.borrow_mut();
            if let Some((last, _)) = queue.back() {
                assert!(
                    *last < timestamp,
                    "edge delivered messages out of timestamp order: {last} then {timestamp}"
                );
            }
            queue.push_back((timestamp, value));
        }
    }

    pub(crate) fn update_remote_ent(&self, ent: Time) {
        self.remote_ent.set(ent + self.delay);
    }

    pub(crate) fn close(&self) {
        self.update_remote_ent(Time::FOREVER);
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
        self.queue
            .borrow()
            .front()
            .map_or(Time::FOREVER, |(timestamp, _)| *timestamp)
            .min(self.remote_ent.get())
    }

    fn ready(&self) -> bool {
        !self.queue.borrow().is_empty()
    }

    fn wake_wakers(&self, time: Time) {
        {
            let mut queue = self.queue.borrow_mut();
            let mut latest = self.latest_value.borrow_mut();
            while queue.front().is_some_and(|(tag, _)| *tag <= time) {
                let (timestamp, value) = queue.pop_front().unwrap();
                self.latest_timestamp.set(timestamp);
                *latest = Some(value);
            }
        }
        self.wake(time);
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
        Ref::filter_map(self.channel.latest_value.borrow(), |latest| latest.as_ref()).ok()
    }

    pub fn timestamp(&self) -> Time {
        self.channel.latest_timestamp.get()
    }

    pub async fn wait(&self) -> Option<Ref<'_, T>> {
        let called_at = self.node_context.now();
        assert!(
            self.channel.latest_timestamp.get() <= called_at,
            "latest timestamp is in the future"
        );
        std::future::poll_fn(|cx| {
            if self.channel.latest_timestamp.get() > called_at {
                return Poll::Ready(self.get());
            };
            if self.channel.queue.borrow().is_empty()
                && self.channel.remote_ent.get() == Time::FOREVER
            {
                return Poll::Ready(None);
            };
            self.node_context.try_advance();
            if self.channel.latest_timestamp.get() > called_at {
                return Poll::Ready(self.get());
            };
            self.channel.register_waker(called_at, cx.waker().clone());
            Poll::Pending
        })
        .await
    }
}

pub(crate) type OutputSender = Box<dyn Fn(Time, Time)>;

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
    fn send(&self, timestamp: Time, ent: Time) {
        for sender in self.senders.borrow().iter() {
            sender(timestamp, ent);
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
}

impl Builder {
    pub(crate) fn new(node_id: NodeId, context: Rc<NodeContext>) -> Self {
        Self {
            node_id,
            next_port_id: 0,
            context,
            ports: HashMap::new(),
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
    #[should_panic(expected = "out of timestamp order")]
    fn out_of_order_push_is_a_transport_bug() {
        let ch = channel();
        ch.push(2 * S, 1);
        ch.push(1 * S, 2);
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
            ch.install_sender(Box::new(move |timestamp, ent| {
                log.borrow_mut().push((edge, timestamp, ent));
            }));
        }

        ch.send(1 * S, 2 * S);
        assert_eq!(*log.borrow(), vec![(1, 1 * S, 2 * S), (2, 1 * S, 2 * S)]);

        ch.close();
        ch.send(3 * S, 4 * S);
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

        /// Let woken tasks run until everything is parked again.
        fn settle(&self) {
            self.runtime.block_on(async {});
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
            while let Some(value) = port.wait().await {
                out.borrow_mut().push((*value, port.timestamp()));
            }
            fin.set(true);
        });
        rig.settle(); // parks: empty queue, upstream open
        assert!(seen.borrow().is_empty());

        // The receive-task contract: push + ENT + try_advance, one stretch.
        channel.push(1 * S, 42);
        channel.update_remote_ent(2 * S);
        rig.ctx.try_advance();
        rig.settle();
        assert_eq!(*seen.borrow(), vec![(42, 1 * S)]);
        assert_eq!(rig.ctx.now(), 1 * S);

        channel.close();
        rig.settle();
        assert!(done.get()); // drained + FOREVER promise = None
    }

    #[test]
    fn a_pending_promise_blocks_until_resolved() {
        let rig = Rig::new();
        let (left, left_ch) = rig.port(0);
        let (_right, right_ch) = rig.port(1);
        let seen = Rc::new(RefCell::new(Vec::new()));

        let out = seen.clone();
        rig.runtime.spawn(async move {
            if let Some(value) = left.wait().await {
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
            if let Some(value) = left.wait().await {
                // Same instant: the sibling's simultaneous value is a
                // settled fact, readable without waiting.
                let sibling = right.get().map(|v| *v);
                out.borrow_mut().push((*value, sibling, right.timestamp()));
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
        assert_eq!(*seen.borrow(), vec![(1, Some(7), 2 * S)]);
        assert_eq!(rig.ctx.now(), 2 * S);
    }

    #[test]
    fn wait_drains_a_closed_queue_before_ending() {
        let rig = Rig::new();
        let (port, channel) = rig.port(0);
        let seen = Rc::new(RefCell::new(Vec::new()));
        let done = Rc::new(Cell::new(false));

        let (out, fin) = (seen.clone(), done.clone());
        rig.runtime.spawn(async move {
            while let Some(value) = port.wait().await {
                out.borrow_mut().push(*value);
            }
            fin.set(true);
        });
        rig.settle();

        channel.push(1 * S, 1);
        channel.push(2 * S, 2);
        channel.close(); // FOREVER promise; two values still queued
        rig.settle();

        // wait's own try_advance cascades: 1s, then 2s, then None.
        assert_eq!(*seen.borrow(), vec![1, 2]);
        assert!(done.get());
        assert_eq!(rig.ctx.now(), 2 * S);
    }
}
