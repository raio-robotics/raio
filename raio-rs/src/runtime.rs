mod waker;

use crate::driver::{Driver, Notifier};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::task::{Context, Waker};
use std::time::Instant;

pub type TaskId = usize;
pub(crate) type BoxFuture<T> = Pin<Box<dyn Future<Output = T>>>;

/// Runs an idle thread's blocking wait, which it receives as its argument.
pub type IdleHook = fn(&mut dyn FnMut());

thread_local! {
    /// Called for each blocking wait on this thread.
    static IDLE_HOOK: Cell<Option<IdleHook>> = const { Cell::new(None) };
}

/// A waker's instruction for scheduling the task that it wakes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WakeKind {
    /// Poll at the current tag, whether that tag is logical or physical.
    Resume,
    /// Request a new physical tag before polling the task.
    Physical,
}

/// Enqueues task wakes and notifies the driver when called from another thread.
#[derive(Clone)]
struct WakeSender {
    sender: Sender<(TaskId, WakeKind)>,
    notifier: Notifier,
}

impl WakeSender {
    fn send(&self, wake: (TaskId, WakeKind)) {
        // A wake that outlives its runtime has no task left to reach.
        if self.sender.send(wake).is_ok() {
            self.notifier.notify();
        }
    }
}

struct Task {
    future: BoxFuture<()>,
    waker: Waker, // Used whenever the executor polls this task.
}

/// Polls futures on one thread and preserves each wake's scheduling policy.
pub struct Runtime {
    wake_receiver: Receiver<(TaskId, WakeKind)>,
    wake_sender: WakeSender,
    /// The wakes that were queued together, in the order their tasks were spawned.
    ready: RefCell<VecDeque<(TaskId, WakeKind)>>,
    tasks: RefCell<HashMap<TaskId, Task>>,
    next_task_id: Cell<TaskId>,
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

impl Runtime {
    pub fn new() -> Self {
        let (sender, wake_receiver) = channel();
        Self {
            wake_receiver,
            wake_sender: WakeSender {
                sender,
                notifier: Driver::notifier(),
            },
            ready: RefCell::new(VecDeque::new()),
            tasks: RefCell::new(HashMap::new()),
            next_task_id: Cell::new(0),
        }
    }

    /// Spawn a user task whose default waker requests physical resumption.
    pub fn spawn(&self, future: impl Future<Output = ()> + 'static) -> TaskId {
        self.spawn_kind(future, WakeKind::Physical)
    }

    /// Spawn an internal task whose default waker resumes at the current tag.
    pub fn spawn_internal(&self, future: impl Future<Output = ()> + 'static) -> TaskId {
        self.spawn_kind(future, WakeKind::Resume)
    }

    fn spawn_kind(&self, future: impl Future<Output = ()> + 'static, kind: WakeKind) -> TaskId {
        let task_id = self.next_task_id.get();
        self.next_task_id.set(task_id + 1);
        let task = Task {
            future: Box::pin(future),
            waker: self.waker_for(task_id, kind),
        };
        self.tasks.borrow_mut().insert(task_id, task);
        // The first poll runs at the current tag.
        self.wake_sender.send((task_id, WakeKind::Resume));
        task_id
    }

    /// Return the current task ID; panic if `cx` does not contain a Raio waker.
    pub fn current_task(cx: &Context<'_>) -> TaskId {
        waker::current_task(cx)
    }

    /// Create a waker with `kind` for this task; panic if `cx` lacks a Raio waker.
    pub fn waker(cx: &Context<'_>, kind: WakeKind) -> Waker {
        waker::with_kind(cx, kind)
    }

    /// Create a waker with the requested policy for a task ID.
    pub(crate) fn waker_for(&self, task: TaskId, kind: WakeKind) -> Waker {
        waker::new(task, kind, self.wake_sender.clone())
    }

    /// Run every blocking wait on this thread inside `hook`.
    pub fn set_idle_hook(hook: IdleHook) {
        IDLE_HOOK.set(Some(hook));
    }

    /// Take the next queued task wake without blocking.
    ///
    /// Wakes queued together are taken in the order their tasks were spawned. A wake queued
    /// while they run waits for the next round, so a task that wakes itself starves no other.
    pub(crate) fn next_wake(&self) -> Option<(TaskId, WakeKind)> {
        let mut ready = self.ready.borrow_mut();
        if ready.is_empty() {
            ready.extend(self.wake_receiver.try_iter());
            // The sort is stable: the wakes of one task keep the order in which they came.
            ready.make_contiguous().sort_by_key(|(task, _)| *task);
        }
        ready.pop_front()
    }

    /// Take a queued wake or wait once in the driver; return `None` if no task woke.
    pub(crate) fn wait_wake_until(&self, alarm_due: Option<Instant>) -> Option<(TaskId, WakeKind)> {
        self.next_wake().or_else(|| {
            let mut wait =
                || Driver::with(|driver| driver.wait(alarm_due)).expect("the I/O poller failed");
            match IDLE_HOOK.get() {
                Some(hook) => hook(&mut wait),
                None => wait(),
            }
            self.next_wake()
        })
    }

    pub(crate) fn finished(&self, task: TaskId) -> bool {
        !self.tasks.borrow().contains_key(&task)
    }

    /// Poll the task once with its default waker; ignore tasks that already finished.
    pub(crate) fn poll(&self, task_id: TaskId) {
        let Some(mut task) = self.tasks.borrow_mut().remove(&task_id) else {
            return;
        };
        let mut context = Context::from_waker(&task.waker);
        if task.future.as_mut().poll(&mut context).is_pending() {
            self.tasks.borrow_mut().insert(task_id, task);
        }
    }

    /// Drop every task that has not finished.
    pub(crate) fn clear(&self) {
        let tasks = std::mem::take(&mut *self.tasks.borrow_mut());
        drop(tasks);
    }

    /// Run a future to completion, polling all wakes without applying tag scheduling.
    pub fn block_on<T: 'static>(&self, future: impl Future<Output = T> + 'static) -> T {
        let output = Rc::new(Cell::new(None));
        let slot = output.clone();
        let root = self.spawn_internal(async move { slot.set(Some(future.await)) });
        while !self.finished(root) {
            if let Some((task, _)) = self.wait_wake_until(None) {
                self.poll(task);
            }
        }
        output.take().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::rc::Rc;
    use std::task::Poll;

    #[test]
    fn runs_non_send_futures() {
        let rt = Runtime::new();
        // Rc allows the test to use a future that cannot move between threads.
        let count = Rc::new(Cell::new(0));
        let c = count.clone();
        rt.spawn(async move { c.set(c.get() + 1) });
        let c = count.clone();
        rt.block_on(async move { c.set(c.get() + 10) });
        assert_eq!(count.get(), 11);
    }

    struct YieldOnce(bool);
    impl Future for YieldOnce {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
            if self.0 {
                return Poll::Ready(());
            }
            self.0 = true;
            cx.waker().wake_by_ref(); // Queue another poll before suspending.
            Poll::Pending
        }
    }

    #[test]
    fn wake_reschedules_a_suspended_task() {
        Runtime::new().block_on(YieldOnce(false));
    }

    #[test]
    fn a_wake_carries_the_kind_of_its_waker() {
        let rt = Runtime::new();
        let task = rt.spawn(async {});
        rt.next_wake().unwrap(); // Discard the wake that schedules the first poll.
        rt.waker_for(task, WakeKind::Physical).wake();
        assert_eq!(rt.next_wake(), Some((task, WakeKind::Physical)));
        assert_eq!(rt.next_wake(), None);
    }

    #[test]
    fn wakes_queued_together_are_taken_in_the_order_their_tasks_were_spawned() {
        let rt = Runtime::new();
        let tasks: Vec<_> = (0..4).map(|_| rt.spawn(std::future::pending())).collect();
        while rt.next_wake().is_some() {} // Discard the wakes that schedule the first polls.
        for task in tasks.iter().rev() {
            rt.waker_for(*task, WakeKind::Resume).wake();
        }
        let taken: Vec<_> = std::iter::from_fn(|| rt.next_wake()).collect();
        let spawned: Vec<_> = tasks.iter().map(|task| (*task, WakeKind::Resume)).collect();
        assert_eq!(taken, spawned);
    }

    #[test]
    fn a_wake_queued_during_a_round_waits_for_the_next_round() {
        let rt = Runtime::new();
        let (first, second) = (rt.spawn(YieldOnce(false)), rt.spawn(async {}));
        assert_eq!(rt.next_wake(), Some((first, WakeKind::Resume)));
        rt.poll(first); // The first task wakes itself.
        // The second task was woken with the first, so it runs before the first runs again.
        assert_eq!(rt.next_wake(), Some((second, WakeKind::Resume)));
        assert_eq!(rt.next_wake().map(|(task, _)| task), Some(first));
    }

    #[test]
    fn a_task_registers_wakers_of_its_own_kind_while_polled() {
        let rt = Rc::new(Runtime::new());
        let resume = Rc::new(RefCell::new(None));
        let slot = resume.clone();
        let task = rt.spawn(async move {
            std::future::poll_fn(|cx| {
                *slot.borrow_mut() = Some(Runtime::waker(cx, WakeKind::Resume));
                Poll::Ready(())
            })
            .await
        });
        rt.poll(rt.next_wake().unwrap().0);
        resume.borrow().as_ref().unwrap().wake_by_ref();
        assert_eq!(rt.next_wake(), Some((task, WakeKind::Resume)));
    }

    #[test]
    fn an_idle_hook_surrounds_each_blocking_wait() {
        thread_local! {
            static CALLS: Cell<usize> = const { Cell::new(0) };
        }
        std::thread::spawn(|| {
            Runtime::set_idle_hook(|wait| {
                CALLS.set(CALLS.get() + 1);
                wait();
            });
            let runtime = Runtime::new();
            let due = Instant::now() + std::time::Duration::from_millis(5);
            assert!(runtime.wait_wake_until(Some(due)).is_none());
            assert_eq!(CALLS.get(), 1);
            assert!(Instant::now() >= due); // The hook ran the wait itself.
        })
        .join()
        .unwrap();
    }

    #[test]
    #[should_panic(expected = "only for futures a raio runtime polls")]
    fn a_waker_for_a_foreign_context_is_a_bug() {
        Runtime::waker(&Context::from_waker(Waker::noop()), WakeKind::Resume);
    }
}
