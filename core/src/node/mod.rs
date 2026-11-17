//! A node: the handle that its tasks hold, and the scheduler that orders its events by timestamp.

mod alarms;
mod foreign;
mod scheduler;

use std::future::Future;
use std::rc::Rc;
use std::task::Poll;

use crate::runtime::{Runtime, WakeKind};
use crate::time::Time;
pub use scheduler::{EventSink, EventSource, NodeScheduler};

pub type NodeId = usize;

/// Provides user tasks with the node's clock and waiting operations.
pub struct Node {
    pub(crate) scheduler: Rc<NodeScheduler>,
}

impl Node {
    /// The node's current timestamp.
    pub fn now(&self) -> Time {
        self.scheduler.now()
    }

    /// The wall-clock time since the node started.
    pub fn physical_now(&self) -> Time {
        self.scheduler.physical_now()
    }

    /// True if the current timestamp was caused by a physical event.
    pub fn is_physical(&self) -> bool {
        self.scheduler.is_physical()
    }

    /// True if the current timestamp's frame has been sent.
    pub fn timestamp_complete(&self) -> bool {
        self.scheduler.timestamp_complete()
    }

    /// Spawn another user task on this node.
    pub fn spawn(&self, task: impl Future<Output = ()> + 'static) {
        self.scheduler.spawn(task);
    }

    /// Stop running the node as soon as the current poll returns.
    pub fn abort(&self) {
        self.scheduler.aborted.set(true);
    }

    /// Ask every node of the app to shut down a microstep after the latest of their current
    /// timestamps.
    pub fn request_shutdown(&self) {
        self.request_shutdown_after(Time::ZERO.next_microstep());
    }

    /// Ask every node of the app to shut down `delay` after the latest of their current timestamps.
    pub fn request_shutdown_after(&self, delay: Time) {
        assert!(delay > Time::ZERO, "a shutdown needs a positive delay");
        self.scheduler.request_shutdown(delay);
    }

    /// Wait for a logical duration and return the timestamp at which it ends.
    pub async fn wait(&self, duration: Time) -> Time {
        assert!(duration > Time::ZERO, "wait needs a positive duration");
        let target = self.scheduler.now() + duration;
        let alarm = self
            .scheduler
            .alarms
            .set(target, self.scheduler.is_physical());
        std::future::poll_fn(|cx| {
            if self.scheduler.now() >= target {
                return Poll::Ready(());
            }
            *alarm.waker.borrow_mut() = Some(Runtime::waker(cx, WakeKind::Resume));
            Poll::Pending
        })
        .await;
        self.scheduler.now()
    }

    /// Await a future and request physical resumption, including when it was already complete.
    pub async fn await_physical<F: Future>(&self, future: F) -> F::Output {
        let output = future.await;
        let mut deferred = false;
        std::future::poll_fn(|cx| {
            if deferred {
                return Poll::Ready(());
            }
            deferred = true;
            self.scheduler.defer(Runtime::current_task(cx));
            Poll::Pending
        })
        .await;
        output
    }
}

/// Controls whether logical execution waits for the wall clock.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExecutionMode {
    /// Execute each safe timestamp once physical time reaches it.
    #[default]
    Realtime,
    /// Execute safe timestamps without wall-clock delays; I/O and ENT promises still apply.
    FastForward,
}
