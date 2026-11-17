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
    pub fn now(&self) -> Time {
        self.scheduler.now()
    }

    pub fn physical_now(&self) -> Time {
        self.scheduler.physical_now()
    }

    /// True if the current tag was caused by a physical event.
    pub fn is_physical(&self) -> bool {
        self.scheduler.is_physical()
    }

    /// True if the current tag's frame has been sent.
    pub fn tag_complete(&self) -> bool {
        self.scheduler.tag_complete()
    }

    /// Spawn another user task on this node.
    pub fn spawn(&self, task: impl Future<Output = ()> + 'static) {
        self.scheduler.spawn(task);
    }

    /// Stop running the node as soon as the current poll returns.
    pub fn abort(&self) {
        self.scheduler.aborted.set(true);
    }

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
    /// Execute each safe tag once physical time reaches it.
    #[default]
    Realtime,
    /// Execute safe tags without wall-clock delays; I/O and ENT promises still apply.
    FastForward,
}
