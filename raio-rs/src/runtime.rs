use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::task::{Context, Wake, Waker};

pub type TaskId = usize;
pub(crate) type BoxFuture<T> = Pin<Box<dyn Future<Output = T>>>;

/// How the node treats a wake-up. The awaited future decides by the waker it registers:
/// raio's futures ask for `Logical` or `Internal`; any other future gets the task's default.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WakeKind {
    /// A logical future
    Logical,
    /// Internal futures (timers, sockets, action handles): run now.
    Internal,
    /// A foreign future completed: run at a fresh physical tag.
    Physical,
}

type WakeSender = Sender<(TaskId, WakeKind)>;

struct TaskWaker {
    task_id: TaskId,
    kind: WakeKind,
    wake_sender: WakeSender,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.wake_sender.send((self.task_id, self.kind)).unwrap();
    }
}

fn waker(task_id: TaskId, kind: WakeKind, wake_sender: WakeSender) -> Waker {
    Waker::from(Arc::new(TaskWaker {
        task_id,
        kind,
        wake_sender,
    }))
}

struct Task {
    future: Pin<Box<dyn Future<Output = ()>>>,
    /// The kind of the task's default waker, `cx.waker()`.
    kind: WakeKind,
}

thread_local! {
    /// The task this thread is polling: what `Runtime::waker` builds wakers for.
    static CURRENT: RefCell<Option<(TaskId, WakeSender)>> = const { RefCell::new(None) };
}

/// A single-threaded executor. It runs tasks and hands out their wake-ups by kind; the
/// node decides what each kind means for time (`NodeContext::run`).
pub struct Runtime {
    wake_receiver: Receiver<(TaskId, WakeKind)>,
    wake_sender: WakeSender,
    tasks: RefCell<HashMap<TaskId, Task>>,
    next_task_id: Cell<TaskId>,
}

impl Runtime {
    pub fn new() -> Self {
        let (wake_sender, wake_receiver) = channel();
        Self {
            wake_receiver,
            wake_sender,
            tasks: RefCell::new(HashMap::new()),
            next_task_id: Cell::new(0),
        }
    }

    /// A user task: a foreign future awaited in it resumes at a physical tag.
    pub fn spawn(&self, future: impl Future<Output = ()> + 'static) -> TaskId {
        self.spawn_kind(future, WakeKind::Physical)
    }

    /// How many user tasks have not finished.
    pub(crate) fn live_user_tasks(&self) -> usize {
        self.tasks
            .borrow()
            .values()
            .filter(|task| task.kind == WakeKind::Physical)
            .count()
    }

    /// One of raio's own tasks: nothing it awaits has a tag of its own.
    pub fn spawn_internal(&self, future: impl Future<Output = ()> + 'static) -> TaskId {
        self.spawn_kind(future, WakeKind::Internal)
    }

    fn spawn_kind(&self, future: impl Future<Output = ()> + 'static, kind: WakeKind) -> TaskId {
        let task_id = self.next_task_id.get();
        self.next_task_id.set(task_id + 1);
        let task = Task {
            future: Box::pin(future),
            kind,
        };
        self.tasks.borrow_mut().insert(task_id, task);
        // The first poll is scheduling, not a resumption from anywhere.
        self.wake_sender
            .send((task_id, WakeKind::Internal))
            .unwrap();
        task_id
    }

    /// The task being polled.
    pub fn current_task() -> TaskId {
        CURRENT.with(|current| {
            current
                .borrow()
                .as_ref()
                .expect("Runtime::current_task is only for futures the runtime is polling")
                .0
        })
    }

    /// A waker of `kind` for the task being polled. raio's futures register these instead
    /// of `cx.waker()`: that is how a future says what its wake-up means.
    pub fn waker(kind: WakeKind) -> Waker {
        CURRENT.with(|current| {
            let (task_id, wake_sender) = current
                .borrow()
                .clone()
                .expect("Runtime::waker is only for futures the runtime is polling");
            waker(task_id, kind, wake_sender)
        })
    }

    /// A waker of `kind` for `task`.
    pub(crate) fn waker_for(&self, task: TaskId, kind: WakeKind) -> Waker {
        waker(task, kind, self.wake_sender.clone())
    }

    /// The next wake-up, if a task has been woken.
    pub(crate) fn next_wake(&self) -> Option<(TaskId, WakeKind)> {
        match self.wake_receiver.try_recv() {
            Ok(wake) => Some(wake),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => unreachable!("the runtime holds a sender"),
        }
    }

    /// The next wake-up, waiting for one.
    pub(crate) fn wait_wake(&self) -> (TaskId, WakeKind) {
        self.wake_receiver.recv().unwrap()
    }

    pub(crate) fn finished(&self, task: TaskId) -> bool {
        !self.tasks.borrow().contains_key(&task)
    }

    /// Poll `task` once, with its default waker. A finished task is skipped: its waker
    /// outlived it.
    pub(crate) fn poll(&self, task_id: TaskId) {
        let Some(mut task) = self.tasks.borrow_mut().remove(&task_id) else {
            return;
        };
        let waker = self.waker_for(task_id, task.kind);
        let mut context = Context::from_waker(&waker);
        CURRENT.with(|current| *current.borrow_mut() = Some((task_id, self.wake_sender.clone())));
        let poll = task.future.as_mut().poll(&mut context);
        CURRENT.with(|current| *current.borrow_mut() = None);
        if poll.is_pending() {
            self.tasks.borrow_mut().insert(task_id, task);
        }
    }

    /// Run `future` to completion with no notion of tags: every wake-up is polled at once.
    pub fn block_on<T: 'static>(&self, future: impl Future<Output = T> + 'static) -> T {
        let output = Rc::new(Cell::new(None));
        let slot = output.clone();
        let root = self.spawn_internal(async move { slot.set(Some(future.await)) });
        loop {
            match self.next_wake() {
                Some((task, _)) => self.poll(task),
                None if self.finished(root) => break,
                None => self.poll(self.wait_wake().0),
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
        let count = Rc::new(Cell::new(0)); // Rc: this future is !Send
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
            cx.waker().wake_by_ref(); // re-queue ourselves, then suspend
            Poll::Pending
        }
    }

    #[test]
    fn wake_reschedules_a_suspended_task() {
        Runtime::new().block_on(async { YieldOnce(false).await });
    }

    #[test]
    fn a_wake_carries_the_kind_of_its_waker() {
        let rt = Runtime::new();
        let task = rt.spawn(async {});
        rt.next_wake().unwrap(); // the first poll is scheduled as internal
        rt.waker_for(task, WakeKind::Physical).wake();
        assert_eq!(rt.next_wake(), Some((task, WakeKind::Physical)));
        assert_eq!(rt.next_wake(), None);
    }

    #[test]
    fn a_task_registers_wakers_of_its_own_kind_while_polled() {
        let rt = Rc::new(Runtime::new());
        let logical = Rc::new(RefCell::new(None));
        let slot = logical.clone();
        let task =
            rt.spawn(async move { *slot.borrow_mut() = Some(Runtime::waker(WakeKind::Logical)) });
        rt.poll(rt.next_wake().unwrap().0);
        logical.borrow().as_ref().unwrap().wake_by_ref();
        assert_eq!(rt.next_wake(), Some((task, WakeKind::Logical)));
    }

    #[test]
    #[should_panic(expected = "only for futures the runtime is polling")]
    fn a_waker_outside_a_task_is_a_bug() {
        Runtime::waker(WakeKind::Logical);
    }
}
