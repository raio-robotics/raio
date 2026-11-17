//! Ports. A channel holds the values of a port, which the node's scheduler and the port's
//! connections share; the port itself is the handle that the node's tasks hold.

use std::cell::{Cell, Ref, RefCell};
use std::collections::BTreeMap;
use std::future::Future;
use std::rc::Rc;
use std::task::{Poll, Waker};

use super::reaction::Trigger;
use super::setting::PortSetting;
use crate::node::{EventSink, EventSource, NodeScheduler};
use crate::runtime::{Runtime, TaskId, WakeKind};
use crate::serializer::Serializer;
use crate::time::Time;
use crate::transport::Transport;

/// Buffers incoming values until the scheduler delivers their timestamps. An input port has one,
/// which its connection feeds, and an action has one, which the threads of its program feed.
pub(crate) struct InputChannel<T> {
    /// The values still to deliver, by timestamp; the flag marks a physical value.
    queue: RefCell<BTreeMap<Time, (T, bool)>>,
    /// The last delivered value and its timestamp.
    latest: RefCell<Option<(Time, T)>>,
    /// Each waiting task has a Resume waker for values and a Physical waker for closure.
    /// The tasks wake in the order they were spawned.
    waiters: RefCell<BTreeMap<TaskId, (Waker, Waker)>>,
    remote_ent: Cell<Time>,
    closed: Cell<bool>,
}

impl<T> InputChannel<T> {
    pub(crate) fn new() -> Self {
        Self {
            queue: RefCell::new(BTreeMap::new()),
            latest: RefCell::new(None),
            waiters: RefCell::new(BTreeMap::new()),
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

    /// Return the latest received timestamp, including values still queued.
    fn last_received(&self) -> Time {
        self.queue
            .borrow()
            .last_key_value()
            .map_or_else(|| self.latest_timestamp(), |(timestamp, _)| *timestamp)
    }

    pub(crate) fn push(&self, timestamp: Time, value: T) {
        assert!(
            timestamp >= self.remote_ent.get(),
            "connection broke its ENT promise: {timestamp} arrived after promising {}",
            self.remote_ent.get()
        );
        assert!(
            timestamp > self.last_received(),
            "connection delivered a logical value at {timestamp} behind one at {}",
            self.last_received()
        );
        self.queue.borrow_mut().insert(timestamp, (value, false));
    }

    /// Queue a physical value, moving its timestamp forward when needed to preserve arrival order.
    pub(crate) fn push_physical(&self, timestamp: Time, value: T) {
        let timestamp = timestamp.max(self.last_received().next_microstep());
        let taken = self.queue.borrow_mut().insert(timestamp, (value, true));
        assert!(taken.is_none(), "two values at {timestamp}");
    }

    pub(crate) fn update_remote_ent(&self, promise: Time) {
        assert!(
            promise >= self.remote_ent.get(),
            "connection lowered its ENT promise from {} to {promise}",
            self.remote_ent.get()
        );
        self.remote_ent.set(promise);
    }

    pub(crate) fn close(&self) {
        self.update_remote_ent(Time::FOREVER);
        self.closed.set(true);
        // Closing an empty input resumes its waiters at physical timestamps.
        if self.queue.borrow().is_empty() {
            for (_, (_, physical)) in self.waiters.take() {
                physical.wake();
            }
        }
    }

    /// The value delivered at the timestamp `now`, if any.
    pub(crate) fn get(&self, now: Time) -> Option<Ref<'_, T>> {
        self.latest()
            .and_then(|(value, timestamp)| (timestamp == now).then_some(value))
    }

    /// The latest value delivered, and its timestamp.
    pub(crate) fn latest(&self) -> Option<(Ref<'_, T>, Time)> {
        let timestamp = self.latest_timestamp();
        Ref::filter_map(self.latest.borrow(), |latest| {
            latest.as_ref().map(|(_, value)| value)
        })
        .ok()
        .map(|value| (value, timestamp))
    }

    /// True once the channel has closed and delivered every value.
    pub(crate) fn drained(&self) -> bool {
        self.closed.get() && self.queue.borrow().is_empty()
    }

    /// Wait for a value after `called_at`; return `None` when the channel has drained.
    pub(crate) fn wait(
        &self,
        called_at: Time,
    ) -> impl Future<Output = Option<(Ref<'_, T>, Time)>> + Unpin {
        assert!(
            self.latest_timestamp() <= called_at,
            "latest timestamp is in the future"
        );
        std::future::poll_fn(move |cx| {
            if self.latest_timestamp() > called_at {
                return Poll::Ready(self.latest());
            };
            if self.drained() {
                return Poll::Ready(None);
            };
            self.waiters.borrow_mut().insert(
                Runtime::current_task(cx),
                (
                    Runtime::waker(cx, WakeKind::Resume),
                    Runtime::waker(cx, WakeKind::Physical),
                ),
            );
            Poll::Pending
        })
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

    /// Return the first queued timestamp once earlier logical arrivals are ruled out.
    fn next_event_timestamp(&self) -> Option<Time> {
        let queue = self.queue.borrow();
        let (head, (_, physical)) = queue.first_key_value()?;
        (!physical || *head < self.remote_ent.get()).then_some(*head)
    }

    fn wake_wakers(&self, time: Time) -> bool {
        let mut physical = false;
        let mut delivered = false;
        {
            let mut queue = self.queue.borrow_mut();
            while queue
                .first_key_value()
                .is_some_and(|(timestamp, _)| *timestamp <= time)
            {
                let (timestamp, (value, is_physical)) = queue.pop_first().unwrap();
                *self.latest.borrow_mut() = Some((timestamp, value));
                physical |= is_physical;
                delivered = true;
            }
        }
        if delivered {
            for (_, (logical, _)) in self.waiters.take() {
                logical.wake();
            }
        }
        physical
    }
}

/// Lets user tasks read or await the values that a connection delivers.
pub struct InputPort<T> {
    pub(crate) channel: Rc<InputChannel<T>>,
    pub(crate) setting: Rc<PortSetting<T>>,
    pub(crate) scheduler: Rc<NodeScheduler>,
}

impl<T> InputPort<T> {
    /// Use this serializer in place of the port's default.
    pub fn set_serializer(&self, serializer: impl Serializer<T> + 'static) {
        let name = serializer.name().to_string();
        self.setting.serializers.set(name, Rc::new(serializer));
    }

    /// Use this transport in place of the port's default.
    pub fn set_transport(&self, transport: impl Transport + 'static) {
        let name = transport.name().to_string();
        self.setting.transports.set(name, Rc::new(transport));
    }

    /// The value delivered at the current timestamp, if any.
    pub fn get(&self) -> Option<Ref<'_, T>> {
        self.channel.get(self.scheduler.now())
    }

    /// The latest value delivered, and its timestamp.
    pub fn latest(&self) -> Option<(Ref<'_, T>, Time)> {
        self.channel.latest()
    }

    /// True once the input has closed and delivered every value.
    pub fn closed(&self) -> bool {
        self.channel.drained()
    }

    /// Wait for a value after this call's timestamp; return `None` when the input closes and
    /// drains.
    pub fn wait(&self) -> impl Future<Output = Option<(Ref<'_, T>, Time)>> + Unpin {
        self.channel.wait(self.scheduler.now())
    }

    /// Wait as `wait` does, for a reaction: true for a value, false once the input has closed.
    pub fn trigger(&self) -> Trigger<'_> {
        let wait = self.wait();
        Box::pin(async move { wait.await.is_some() })
    }
}

/// Sends one timestamp's frame, with its value if set, over one connection.
pub(crate) type OutputSender<T> = Box<dyn Fn(Time, Time, bool, Option<&T>)>;

/// Holds the current timestamp's output value and the callbacks that transmit its frame.
pub(crate) struct OutputChannel<T> {
    pub(crate) cell: RefCell<Option<T>>,
    pub(crate) senders: RefCell<Vec<OutputSender<T>>>,
    pub(crate) setting: PortSetting<T>,
}

impl<T> OutputChannel<T> {
    pub(crate) fn new(setting: PortSetting<T>) -> Self {
        Self {
            cell: RefCell::new(None),
            senders: RefCell::new(Vec::new()),
            setting,
        }
    }
}

impl<T> EventSink for OutputChannel<T> {
    fn send(&self, timestamp: Time, ent: Time, physical: bool) {
        // Every connection of the output sends the same value.
        let value = self.cell.borrow_mut().take();
        for sender in self.senders.borrow().iter() {
            sender(timestamp, ent, physical, value.as_ref());
        }
    }

    fn close(&self) {
        self.senders.borrow_mut().clear();
    }
}

/// Lets user tasks set the value sent when the current timestamp completes.
pub struct OutputPort<T> {
    pub(crate) channel: Rc<OutputChannel<T>>,
    pub(crate) scheduler: Rc<NodeScheduler>,
}

impl<T> OutputPort<T> {
    /// Use this serializer in place of the port's default.
    pub fn set_serializer(&self, serializer: impl Serializer<T> + 'static) {
        let name = serializer.name().to_string();
        let serializers = &self.channel.setting.serializers;
        serializers.set(name, Rc::new(serializer));
    }

    /// Have this serializer too, for the connections whose inputs use it.
    pub fn add_serializer(&self, serializer: impl Serializer<T> + 'static) {
        let name = serializer.name().to_string();
        let serializers = &self.channel.setting.serializers;
        serializers.add(name, Rc::new(serializer));
    }

    /// Use this transport in place of the port's default.
    pub fn set_transport(&self, transport: impl Transport + 'static) {
        let name = transport.name().to_string();
        let transports = &self.channel.setting.transports;
        transports.set(name, Rc::new(transport));
    }

    /// Set the value that the node sends when the current timestamp completes.
    pub fn set(&self, value: T) {
        assert!(
            !self.scheduler.timestamp_complete(),
            "output set at {}, a timestamp the node already completed; \
             timestamp 0 completes at startup, so wait first",
            self.scheduler.now()
        );
        *self.channel.cell.borrow_mut() = Some(value);
    }

    /// Clear the value set at the current timestamp.
    pub fn reset(&self) {
        *self.channel.cell.borrow_mut() = None;
    }
}

#[cfg(test)]
impl InputPort<i64> {
    /// Create an input that a test feeds through the returned channel.
    pub(crate) fn local(scheduler: &Rc<NodeScheduler>) -> (Self, Rc<InputChannel<i64>>) {
        let channel = Rc::new(InputChannel::new());
        scheduler.add_source(channel.clone());
        let id = crate::coordinator::PortId {
            node_id: 0,
            port_id: 0,
        };
        let port = InputPort {
            channel: channel.clone(),
            setting: Rc::new(PortSetting::new(id)),
            scheduler: scheduler.clone(),
        };
        (port, channel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::Builder;
    use crate::serializer::tests::Decimal;
    use crate::time::S;
    use crate::transport::tests::Loopback;

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
    fn latch_folds_timestamps_up_to_time() {
        let ch = channel();
        ch.push(1 * S, 1);
        ch.push(2 * S, 2);
        ch.push(3 * S, 3);

        ch.wake_wakers(2 * S);
        // The latest value through 2s is retained.
        assert_eq!(*ch.latest.borrow(), Some((2 * S, 2)));
        assert_eq!(ch.queue.borrow().len(), 1); // The 3s value remains queued.

        ch.wake_wakers(2 * S); // Repeating delivery at the same timestamp has no effect.
        assert_eq!(*ch.latest.borrow(), Some((2 * S, 2)));
    }

    #[test]
    #[should_panic(expected = "broke its ENT promise")]
    fn a_push_before_the_promise_is_a_connection_bug() {
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
    fn a_physical_value_at_a_taken_timestamp_takes_the_next_microstep() {
        let ch = channel();
        ch.push(3 * S, 1);
        ch.push_physical(3 * S, 2);

        ch.wake_wakers(3 * S);
        // Logical values retain their sender's timestamp.
        assert_eq!(*ch.latest.borrow(), Some((3 * S, 1)));
        ch.wake_wakers((3 * S).next_microstep());
        assert_eq!(*ch.latest.borrow(), Some(((3 * S).next_microstep(), 2)));
    }

    #[test]
    #[should_panic(expected = "behind one at")]
    fn a_logical_value_behind_a_physical_one_is_a_connection_bug() {
        let ch = channel();
        ch.push_physical(3 * S, 2);
        // A logical value cannot arrive behind an earlier value on the same connection.
        ch.push(3 * S, 1);
    }

    #[test]
    fn a_logical_head_is_ready_regardless_of_the_promise() {
        let ch = channel();
        ch.push(1 * S, 1); // Ordered delivery rules out an earlier logical value.
        assert_eq!(ch.next_event_timestamp(), Some(1 * S));
    }

    #[test]
    fn a_physical_head_waits_for_the_logical_promise() {
        let ch = channel();
        ch.push_physical(3 * S, 7);
        ch.update_remote_ent(2 * S); // The upstream may still send a logical value before 3s.
        assert_eq!(ch.next_event_timestamp(), None);
        assert_eq!(ch.ent(), 2 * S);

        ch.update_remote_ent(4 * S); // The new ENT rules out logical values at or before 3s.
        assert_eq!(ch.next_event_timestamp(), Some(3 * S));
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
        assert_eq!(ch.next_event_timestamp(), Some(3 * S));
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
    fn a_port_uses_bincode_over_tcp_until_set_otherwise() {
        let scheduler = Rc::new(NodeScheduler::new(Rc::new(Runtime::new())));
        let mut builder = Builder::new(0, scheduler);
        let _first: InputPort<i64> = builder.input();
        let out: OutputPort<i64> = builder.output();
        let described = out.channel.setting.describe();
        // A port is known by its position among the ports its node built.
        assert_eq!(described.id, 1);
        assert_eq!(described.serializers, ["bincode"]);
        assert_eq!(described.transports, ["tcp"]);

        out.add_serializer(Decimal);
        // An output keeps a serializer for each choice that its connections make.
        let serializers = out.channel.setting.describe().serializers;
        assert_eq!(serializers, ["bincode", Decimal.name()]);

        out.set_serializer(Decimal);
        out.set_transport(Loopback);
        let described = out.channel.setting.describe();
        assert_eq!(described.serializers, [Decimal.name()]);
        assert_eq!(described.transports, [Transport::name(&Loopback)]);
    }

    #[test]
    fn output_channel_fans_out_to_every_sender() {
        let ch = OutputChannel::<i64>::new(PortSetting::new(crate::coordinator::PortId {
            node_id: 0,
            port_id: 0,
        }));
        let log = Rc::new(RefCell::new(Vec::new()));
        for connection in [1, 2] {
            let log = log.clone();
            ch.senders
                .borrow_mut()
                .push(Box::new(move |timestamp, ent, physical, value| {
                    log.borrow_mut()
                        .push((connection, timestamp, ent, physical, value.copied()));
                }));
        }

        *ch.cell.borrow_mut() = Some(7);
        ch.send(1 * S, 2 * S, true);
        // Both connections carry the timestamp's value.
        assert_eq!(
            *log.borrow(),
            vec![
                (1, 1 * S, 2 * S, true, Some(7)),
                (2, 1 * S, 2 * S, true, Some(7))
            ]
        );

        ch.send(2 * S, 3 * S, false);
        // A timestamp without a value still sends its frame on every connection.
        assert_eq!(
            log.borrow()[2..],
            [
                (1, 2 * S, 3 * S, false, None),
                (2, 2 * S, 3 * S, false, None)
            ]
        );

        ch.close();
        ch.send(3 * S, 4 * S, false);
        assert_eq!(log.borrow().len(), 4); // Closing the channel removes all sender callbacks.
    }

    // These integration tests drive port futures through the scheduler.

    use crate::runtime::Runtime;
    use std::cell::Cell;

    struct Rig {
        scheduler: Rc<NodeScheduler>,
    }

    impl Rig {
        fn new() -> Self {
            let scheduler = NodeScheduler::fast_forward();
            Self { scheduler }
        }

        fn port(&self) -> (InputPort<i64>, Rc<InputChannel<i64>>) {
            InputPort::local(&self.scheduler)
        }

        /// Run tasks and advance timestamps until no immediate work remains.
        fn settle(&self) {
            self.scheduler.run_until_idle();
        }
    }

    #[test]
    fn wait_delivers_after_arrival_and_ends_on_close() {
        let rig = Rig::new();
        let (port, channel) = rig.port();
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
    fn tasks_that_wait_for_one_input_resume_in_the_order_they_were_spawned() {
        let rig = Rig::new();
        let (port, channel) = rig.port();
        let port = Rc::new(port);
        let order = Rc::new(RefCell::new(Vec::new()));

        for task in 0..16 {
            let (port, order) = (port.clone(), order.clone());
            rig.scheduler.spawn(async move {
                port.wait().await;
                order.borrow_mut().push(task);
            });
        }
        rig.settle();

        channel.push(1 * S, 42);
        channel.update_remote_ent(2 * S);
        rig.settle();
        assert_eq!(*order.borrow(), (0..16).collect::<Vec<_>>());
    }

    #[test]
    fn a_pending_promise_blocks_until_resolved() {
        let rig = Rig::new();
        let (left, left_ch) = rig.port();
        let (_right, right_ch) = rig.port();
        let seen = Rc::new(RefCell::new(Vec::new()));

        let out = seen.clone();
        rig.scheduler.spawn(async move {
            if let Some((value, _)) = left.wait().await {
                out.borrow_mut().push(*value);
            }
        });
        rig.settle();

        // The right input may still deliver a value at the left input's queued timestamp.
        left_ch.push(2 * S, 1);
        left_ch.update_remote_ent(4 * S);
        right_ch.update_remote_ent(2 * S);
        rig.scheduler.try_advance();
        rig.settle();
        // An ENT equal to the pending timestamp still blocks delivery.
        assert!(seen.borrow().is_empty());
        assert_eq!(rig.scheduler.now(), Time::ZERO);

        // Advancing the right input's ENT past 2s allows delivery.
        right_ch.update_remote_ent(4 * S);
        rig.scheduler.try_advance();
        rig.settle();
        assert_eq!(*seen.borrow(), vec![1]);
        assert_eq!(rig.scheduler.now(), 2 * S);
    }

    #[test]
    fn simultaneous_timestamps_settle_in_one_instant() {
        let rig = Rig::new();
        let (left, left_ch) = rig.port();
        let (right, right_ch) = rig.port();
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
        let (right, right_ch) = rig.port();
        let (_left, left_ch) = rig.port();
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
        let (port, channel) = rig.port();
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
        let (port, channel) = rig.port();
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

        // The scheduler delivers both queued timestamps before the waiter observes closure.
        assert_eq!(*seen.borrow(), vec![1, 2]);
        assert!(done.get());
        assert_eq!(rig.scheduler.now(), 2 * S);
    }

    #[test]
    fn an_input_is_closed_once_its_queue_has_drained() {
        let rig = Rig::new();
        let (port, channel) = rig.port();
        channel.push(1 * S, 1);
        channel.close();
        assert!(!port.closed()); // The queued value is still to come.
        rig.settle();
        assert!(port.closed());
    }

    #[test]
    fn a_close_resumes_the_waiter_at_a_physical_timestamp() {
        let rig = Rig::new();
        let (port, channel) = rig.port();
        let seen = Rc::new(Cell::new(None));

        let (out, scheduler) = (seen.clone(), rig.scheduler.clone());
        rig.scheduler.spawn(async move {
            assert!(port.wait().await.is_none());
            out.set(Some((scheduler.now(), scheduler.is_physical())));
        });
        rig.settle(); // The waiter is suspended at the initial timestamp.

        channel.close(); // Closing an empty input schedules a physical resumption.
        rig.settle();
        let (timestamp, physical) = seen.get().unwrap();
        assert!(timestamp > Time::ZERO, "{timestamp}");
        assert!(physical);
    }

    #[test]
    fn a_forever_promise_is_not_a_close() {
        let rig = Rig::new();
        let (port, channel) = rig.port();
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
    fn a_physical_value_marks_its_timestamp_and_only_its_timestamp() {
        let rig = Rig::new();
        let (_port, channel) = rig.port();

        channel.push_physical(3 * S, 7);
        channel.update_remote_ent(4 * S);
        rig.scheduler.try_advance();
        assert_eq!(rig.scheduler.now(), 3 * S);
        assert!(rig.scheduler.is_physical());

        channel.push(5 * S, 1);
        channel.update_remote_ent(6 * S);
        rig.scheduler.try_advance();
        assert_eq!(rig.scheduler.now(), 5 * S);
        // A later logical event clears the timestamp's physical flag.
        assert!(!rig.scheduler.is_physical());
    }
}
