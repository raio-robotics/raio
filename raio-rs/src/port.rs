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
use crate::node::{EventSink, EventSource, ExecutionMode, Node, NodeId, NodeScheduler};
use crate::runtime::{BoxFuture, Runtime, TaskId, WakeKind};
use crate::time::Time;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct PortId {
    pub node_id: NodeId,
    pub port_id: usize,
}

pub(crate) type InputQueue<T> = RefCell<BTreeMap<Time, (T, bool)>>;

/// Buffers incoming values until the scheduler delivers their tags.
pub(crate) struct InputChannel<T> {
    queue: InputQueue<T>,
    /// The last delivered value and its tag.
    latest: RefCell<Option<(Time, T)>>,
    /// Each waiting task has a Resume waker for values and a Physical waker for closure.
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

    /// Return the latest received tag, including values still queued.
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

    /// Queue a physical value, moving its tag forward when needed to preserve arrival order.
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
        // Closing an empty input resumes its waiters at physical tags.
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
        // ENT is the earlier of the first queued logical value and the upstream promise.
        self.queue
            .borrow()
            .iter()
            .find(|(_, (_, physical))| !physical)
            .map_or(Time::FOREVER, |(timestamp, _)| *timestamp)
            .min(self.remote_ent.get())
    }

    /// Return the first queued tag once earlier logical arrivals are ruled out.
    fn next_event_tag(&self) -> Option<Time> {
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
            self.wake();
        }
        physical
    }
}

/// Lets user tasks read or await values delivered by an input channel.
pub struct InputPort<T> {
    pub(crate) id: PortId,
    pub(crate) channel: Rc<InputChannel<T>>,
    pub(crate) scheduler: Rc<NodeScheduler>,
}

impl<T> InputPort<T> {
    /// The value delivered at the current tag, if any.
    pub fn get(&self) -> Option<Ref<'_, T>> {
        let now = self.scheduler.now();
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

    /// Wait for a value after this call's tag; return `None` when the input closes and drains.
    pub fn wait(&self) -> impl Future<Output = Option<(Ref<'_, T>, Time)>> {
        let called_at = self.scheduler.now();
        assert!(
            self.channel.latest_timestamp() <= called_at,
            "latest timestamp is in the future"
        );
        std::future::poll_fn(move |cx| {
            if self.channel.latest_timestamp() > called_at {
                return Poll::Ready(self.latest());
            };
            if self.channel.queue.borrow().is_empty() && self.channel.closed.get() {
                return Poll::Ready(None);
            };
            self.channel.waiters.borrow_mut().insert(
                Runtime::current_task(cx),
                (
                    Runtime::waker(cx, WakeKind::Resume),
                    Runtime::waker(cx, WakeKind::Physical),
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

/// Holds the current tag's output value and the callbacks that transmit its frame.
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

impl<T> EventSink for OutputChannel<T> {
    fn send(&self, timestamp: Time, ent: Time, physical: bool) {
        for sender in self.senders.borrow().iter() {
            sender(timestamp, ent, physical);
        }
    }

    fn close(&self) {
        self.senders.borrow_mut().clear();
    }
}

/// Lets user tasks set the value sent when the current tag completes.
pub struct OutputPort<T> {
    pub(crate) id: PortId,
    pub(crate) channel: Rc<OutputChannel<T>>,
    pub(crate) scheduler: Rc<NodeScheduler>,
}

impl<T> OutputPort<T> {
    pub fn set(&self, value: T) {
        assert!(
            !self.scheduler.tag_complete(),
            "output set at {}, a tag the node already completed; tag 0 completes at startup, so wait first",
            self.scheduler.now()
        );
        *self.channel.cell.borrow_mut() = Some(value);
    }

    pub fn reset(&self) {
        *self.channel.cell.borrow_mut() = None;
    }
}

/// Creates a node's ports, actions, and clock handles using one shared scheduler.
pub struct Builder {
    node_id: NodeId,
    pub(crate) next_port_id: usize,
    scheduler: Rc<NodeScheduler>,
    pub(crate) ports: HashMap<PortId, Rc<dyn Any>>,
    /// Every input port must have exactly one upstream connection.
    pub(crate) inputs: Vec<PortId>,
    /// Internal tasks that forward action values into input channels.
    pub(crate) tasks: Vec<BoxFuture<()>>,
}

impl Builder {
    pub(crate) fn new(node_id: NodeId, scheduler: Rc<NodeScheduler>) -> Self {
        Self {
            node_id,
            next_port_id: 0,
            scheduler,
            ports: HashMap::new(),
            inputs: Vec::new(),
            tasks: Vec::new(),
        }
    }

    pub fn input<T: 'static>(&mut self) -> InputPort<T> {
        let id = self.next_port_id();
        let channel = Rc::new(InputChannel::new());
        self.scheduler.add_source(channel.clone());
        self.ports.insert(id, channel.clone() as Rc<dyn Any>);
        self.inputs.push(id);
        InputPort {
            id,
            channel,
            scheduler: self.scheduler.clone(),
        }
    }

    pub fn action<T: Send + 'static>(&mut self) -> Action<T> {
        let id = self.next_port_id();
        let channel = Rc::new(InputChannel::new());
        // Action values are physical, so they do not limit ENT.
        channel.update_remote_ent(Time::FOREVER);
        self.scheduler.add_source(channel.clone());
        let (sender, mut receiver) = unbounded::<T>();
        let (action_channel, scheduler) = (channel.clone(), self.scheduler.clone());
        self.tasks.push(Box::pin(async move {
            while let Some(value) = receiver.next().await {
                action_channel.push_physical(scheduler.mint_physical_tag(), value);
            }
            action_channel.close(); // Dropping the last producer handle closes the action.
        }));
        Action {
            port: InputPort {
                id,
                channel,
                scheduler: self.scheduler.clone(),
            },
            handles: RefCell::new(Handles::Unused(Arc::new(sender))),
        }
    }

    /// Select `Realtime` (the default) or `FastForward` pacing for this node.
    pub fn execution_mode(&mut self, execution_mode: ExecutionMode) {
        self.scheduler.set_execution_mode(execution_mode);
    }

    pub fn node(&mut self) -> Node {
        Node {
            scheduler: self.scheduler.clone(),
        }
    }

    pub fn output<T: 'static>(&mut self) -> OutputPort<T> {
        let id = self.next_port_id();
        let channel = Rc::new(OutputChannel::new());
        self.scheduler.add_sink(channel.clone());
        self.ports.insert(id, channel.clone() as Rc<dyn Any>);
        OutputPort {
            id,
            channel,
            scheduler: self.scheduler.clone(),
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
        assert_eq!(ch.ent(), Time::ZERO); // Until the upstream sends a promise, ENT starts at zero.

        ch.update_remote_ent(1 * S);
        assert_eq!(ch.ent(), 1 * S);

        ch.push(2 * S, 42);
        // The upstream may still send a logical value before the queued value.
        assert_eq!(ch.ent(), 1 * S);

        ch.update_remote_ent(5 * S);
        assert_eq!(ch.ent(), 2 * S); // The first queued value now determines ENT.
    }

    #[test]
    fn latch_folds_tags_up_to_time() {
        let ch = channel();
        ch.push(1 * S, 1);
        ch.push(2 * S, 2);
        ch.push(3 * S, 3);

        ch.wake_wakers(2 * S);
        // The latest value through 2s is retained.
        assert_eq!(*ch.latest.borrow(), Some((2 * S, 2)));
        assert_eq!(ch.queue.borrow().len(), 1); // The 3s value remains queued.

        ch.wake_wakers(2 * S); // Repeating delivery at the same tag has no effect.
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
        // Arrival order moves this physical value past the earlier 5s value.
        ch.push_physical(3 * S, 30);

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
        // Logical values retain their sender's tag.
        assert_eq!(*ch.latest.borrow(), Some((3 * S, 1)));
        ch.wake_wakers((3 * S).next_microstep());
        assert_eq!(*ch.latest.borrow(), Some(((3 * S).next_microstep(), 2)));
    }

    #[test]
    #[should_panic(expected = "behind one at")]
    fn a_logical_value_behind_a_physical_one_is_an_edge_bug() {
        let ch = channel();
        ch.push_physical(3 * S, 2);
        // A logical value cannot arrive behind an earlier value on the same edge.
        ch.push(3 * S, 1);
    }

    #[test]
    fn a_logical_head_is_ready_regardless_of_the_promise() {
        let ch = channel();
        ch.push(1 * S, 1); // Ordered delivery rules out an earlier logical value.
        assert_eq!(ch.next_event_tag(), Some(1 * S));
    }

    #[test]
    fn a_physical_head_waits_for_the_logical_promise() {
        let ch = channel();
        ch.push_physical(3 * S, 7);
        ch.update_remote_ent(2 * S); // The upstream may still send a logical value before 3s.
        assert_eq!(ch.next_event_tag(), None);
        assert_eq!(ch.ent(), 2 * S);

        ch.update_remote_ent(4 * S); // The new ENT rules out logical values at or before 3s.
        assert_eq!(ch.next_event_tag(), Some(3 * S));
        assert_eq!(ch.ent(), 4 * S); // The physical value does not lower ENT.
    }

    #[test]
    fn ent_promises_only_logical_events() {
        let ch = channel();
        ch.push_physical(3 * S, 30); // This frame carries a physical value.
        ch.update_remote_ent(5 * S);
        ch.push(7 * S, 70); // The next frame carries a logical value.
        ch.update_remote_ent(10 * S);

        // The physical value is delivered before the logical value.
        assert_eq!(ch.next_event_tag(), Some(3 * S));
        assert_eq!(ch.ent(), 7 * S); // Only the queued logical value lowers ENT.
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
        assert_eq!(log.borrow().len(), 2); // Closing the channel removes all sender callbacks.
    }

    // These integration tests drive port futures through the scheduler.

    use crate::runtime::Runtime;
    use std::cell::Cell;

    struct Rig {
        scheduler: Rc<NodeScheduler>,
    }

    impl Rig {
        fn new() -> Self {
            let scheduler = Rc::new(NodeScheduler::with_execution_mode(
                Rc::new(Runtime::new()),
                ExecutionMode::FastForward,
            ));
            Self { scheduler }
        }

        fn port(&self, port_id: usize) -> (InputPort<i64>, Rc<InputChannel<i64>>) {
            let channel = Rc::new(InputChannel::new());
            self.scheduler.add_source(channel.clone());
            let port = InputPort {
                id: PortId {
                    node_id: 0,
                    port_id,
                },
                channel: channel.clone(),
                scheduler: self.scheduler.clone(),
            };
            (port, channel)
        }

        /// Run tasks and advance tags until no immediate work remains.
        fn settle(&self) {
            self.scheduler.run_until_idle();
        }
    }

    #[test]
    fn wait_delivers_after_arrival_and_ends_on_close() {
        let rig = Rig::new();
        let (port, channel) = rig.port(0);
        let seen = Rc::new(RefCell::new(Vec::new()));
        let done = Rc::new(Cell::new(false));

        let (out, fin) = (seen.clone(), done.clone());
        rig.scheduler.spawn(async move {
            while let Some((value, timestamp)) = port.wait().await {
                out.borrow_mut().push((*value, timestamp));
            }
            fin.set(true);
        });
        rig.settle(); // The task waits because the input is empty and still open.
        assert!(seen.borrow().is_empty());

        // Receiving a value and its ENT allows the idle scheduler to advance.
        channel.push(1 * S, 42);
        channel.update_remote_ent(2 * S);
        rig.settle();
        assert_eq!(*seen.borrow(), vec![(42, 1 * S)]);
        assert_eq!(rig.scheduler.now(), 1 * S);

        channel.close();
        rig.settle();
        // The waiter finishes after consuming all values from the closed input.
        assert!(done.get());
    }

    #[test]
    fn a_pending_promise_blocks_until_resolved() {
        let rig = Rig::new();
        let (left, left_ch) = rig.port(0);
        let (_right, right_ch) = rig.port(1);
        let seen = Rc::new(RefCell::new(Vec::new()));

        let out = seen.clone();
        rig.scheduler.spawn(async move {
            if let Some((value, _)) = left.wait().await {
                out.borrow_mut().push(*value);
            }
        });
        rig.settle();

        // The right input may still deliver a value at the left input's queued tag.
        left_ch.push(2 * S, 1);
        left_ch.update_remote_ent(4 * S);
        right_ch.update_remote_ent(2 * S);
        rig.scheduler.try_advance();
        rig.settle();
        assert!(seen.borrow().is_empty()); // An ENT equal to the pending tag still blocks delivery.
        assert_eq!(rig.scheduler.now(), Time::ZERO);

        // Advancing the right input's ENT past 2s allows delivery.
        right_ch.update_remote_ent(4 * S);
        rig.scheduler.try_advance();
        rig.settle();
        assert_eq!(*seen.borrow(), vec![1]);
        assert_eq!(rig.scheduler.now(), 2 * S);
    }

    #[test]
    fn simultaneous_tags_settle_in_one_instant() {
        let rig = Rig::new();
        let (left, left_ch) = rig.port(0);
        let (right, right_ch) = rig.port(1);
        let seen = Rc::new(RefCell::new(Vec::new()));

        let out = seen.clone();
        rig.scheduler.spawn(async move {
            if let Some((value, _)) = left.wait().await {
                // Both input values must be visible before either waiting task runs.
                let sibling = right.latest().map(|(v, timestamp)| (*v, timestamp));
                out.borrow_mut().push((*value, sibling));
            }
        });
        rig.settle();

        left_ch.push(2 * S, 1);
        left_ch.update_remote_ent(4 * S);
        right_ch.push(2 * S, 7);
        right_ch.update_remote_ent(4 * S);
        rig.scheduler.try_advance();
        rig.settle();

        // A single clock advance delivers both values at 2s.
        assert_eq!(*seen.borrow(), vec![(1, Some((7, 2 * S)))]);
        assert_eq!(rig.scheduler.now(), 2 * S);
    }

    #[test]
    fn wait_resolves_via_latest_even_after_further_advances() {
        let rig = Rig::new();
        let (right, right_ch) = rig.port(0);
        let (_left, left_ch) = rig.port(1);
        let seen = Rc::new(RefCell::new(Vec::new()));

        let out = seen.clone();
        rig.scheduler.spawn(async move {
            if let Some((value, timestamp)) = right.wait().await {
                out.borrow_mut().push((*value, timestamp));
            }
        });
        rig.settle();

        right_ch.push(3 * S, 30);
        right_ch.update_remote_ent(10 * S);
        left_ch.push(5 * S, 50);
        left_ch.update_remote_ent(10 * S);
        rig.scheduler.try_advance();
        rig.scheduler.try_advance();
        assert_eq!(rig.scheduler.now(), 5 * S);
        rig.settle();

        assert_eq!(*seen.borrow(), vec![(30, 3 * S)]);
    }

    #[test]
    fn wait_baseline_is_at_call_not_first_poll() {
        let rig = Rig::new();
        let (port, channel) = rig.port(0);
        let scheduler = rig.scheduler.clone();
        let seen = Rc::new(RefCell::new(None));

        let out = seen.clone();
        rig.scheduler.spawn(async move {
            let pending = port.wait();
            // A value may arrive after wait() is called but before its first poll.
            channel.push(1 * S, 7);
            channel.close();
            scheduler.try_advance();
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
        rig.scheduler.spawn(async move {
            while let Some((value, _)) = port.wait().await {
                out.borrow_mut().push(*value);
            }
            fin.set(true);
        });
        rig.settle();

        channel.push(1 * S, 1);
        channel.push(2 * S, 2);
        channel.close(); // Closing the input preserves its queued values.
        rig.settle();

        // The scheduler delivers both queued tags before the waiter observes closure.
        assert_eq!(*seen.borrow(), vec![1, 2]);
        assert!(done.get());
        assert_eq!(rig.scheduler.now(), 2 * S);
    }

    #[test]
    fn a_close_resumes_the_waiter_at_a_physical_tag() {
        let rig = Rig::new();
        let (port, channel) = rig.port(0);
        let seen = Rc::new(Cell::new(None));

        let (out, scheduler) = (seen.clone(), rig.scheduler.clone());
        rig.scheduler.spawn(async move {
            assert!(port.wait().await.is_none());
            out.set(Some((scheduler.now(), scheduler.is_physical())));
        });
        rig.settle(); // The waiter is suspended at the initial tag.

        channel.close(); // Closing an empty input schedules a physical resumption.
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

        // An ENT of FOREVER rules out logical values but does not close the input.
        channel.update_remote_ent(Time::FOREVER);
        let fin = done.clone();
        rig.scheduler.spawn(async move {
            assert!(port.wait().await.is_none());
            fin.set(true);
        });
        rig.settle();
        assert!(!done.get()); // The open input can still receive physical values.

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
        rig.scheduler.try_advance();
        assert_eq!(rig.scheduler.now(), 3 * S);
        assert!(rig.scheduler.is_physical());

        channel.push(5 * S, 1);
        channel.update_remote_ent(6 * S);
        rig.scheduler.try_advance();
        assert_eq!(rig.scheduler.now(), 5 * S);
        // A later logical event clears the tag's physical flag.
        assert!(!rig.scheduler.is_physical());
    }
}
