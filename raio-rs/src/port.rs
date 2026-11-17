use crate::node::NodeContext;
use crate::node::{EventSource, NodeId};
use crate::time::Time;
use std::cell::Cell;
use std::cell::Ref;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::option::Option;
use std::rc::Rc;
use std::task::Waker;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
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
}

impl<T> InputPort<T> {
    /// Read the latest value from the port
    pub fn get(&self) -> Option<Ref<'_, T>> {
        Ref::filter_map(self.channel.latest_value.borrow(), |latest| latest.as_ref()).ok()
    }

    pub fn timestamp(&self) -> Time {
        self.channel.latest_timestamp.get()
    }
}

pub(crate) type OutputCell<T> = Rc<RefCell<Option<T>>>;

pub struct OutputPort<T> {
    pub(crate) id: PortId,
    pub(crate) cell: OutputCell<T>,
}

impl<T> OutputPort<T> {
    pub fn set(&self, value: T) {
        *self.cell.borrow_mut() = Some(value);
    }

    pub fn reset(&self) {
        *self.cell.borrow_mut() = None;
    }
}

pub struct Builder {
    node_id: NodeId,
    next_port_id: usize,
    context: Rc<NodeContext>,
}

impl Builder {
    pub(crate) fn new(node_id: NodeId, context: Rc<NodeContext>) -> Self {
        Self {
            node_id,
            next_port_id: 0,
            context,
        }
    }

    pub(crate) fn port_count(&self) -> usize {
        self.next_port_id
    }

    pub fn input<T: 'static>(&mut self) -> InputPort<T> {
        let channel = Rc::new(InputChannel::new(Time::ZERO));
        self.context.register_source(channel.clone());
        InputPort {
            id: self.next_port_id(),
            channel,
        }
    }

    pub fn output<T>(&mut self) -> OutputPort<T> {
        OutputPort {
            id: self.next_port_id(),
            cell: Rc::new(RefCell::new(None)),
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
}
