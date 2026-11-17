use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::task::{Context, Wake, Waker};

use crate::time::Time;
use std::sync::mpsc::TryRecvError;

type TaskId = usize;

struct TaskWaker {
    task_id: TaskId,
    task_sender: Sender<TaskId>,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.task_sender.send(self.task_id).unwrap();
    }
}

pub struct Runtime {
    task_receiver: Receiver<TaskId>,
    task_sender: Sender<TaskId>,
    tasks: RefCell<HashMap<usize, Pin<Box<dyn Future<Output = ()>>>>>,
    next_task_id: Cell<TaskId>,
    logical_time: Cell<Time>,
}

impl Runtime {
    pub fn new() -> Self {
        let (task_sender, task_receiver) = channel();
        Self {
            task_sender,
            task_receiver,
            tasks: RefCell::new(HashMap::new()),
            next_task_id: Cell::new(0),
            logical_time: Cell::new(Time::ZERO),
        }
    }

    pub fn advance_to(&self, time: Time) {
        let now = self.logical_time.get();
        assert!(time > now, "Cannot advance to a time in the past");
        self.logical_time.set(time);
    }

    pub fn now(&self) -> Time {
        self.logical_time.get()
    }

    pub fn spawn(&self, future: impl Future<Output = ()> + 'static) -> TaskId {
        let task_id = self.next_task_id.get();
        self.next_task_id.set(task_id + 1);
        self.tasks.borrow_mut().insert(task_id, Box::pin(future));
        self.task_sender.send(task_id).unwrap();
        task_id
    }

    pub fn block_on(&self, future: impl Future<Output = ()> + 'static) {
        self.block_on_with_idle(future, || {});
    }

    pub fn block_on_with_idle(
        &self,
        future: impl Future<Output = ()> + 'static,
        on_idle: impl Fn(),
    ) {
        let root_task_id = self.spawn(future);
        loop {
            let task_id = match self.task_receiver.try_recv() {
                Ok(task_id) => task_id,
                Err(TryRecvError::Empty) => {
                    // Every woken task has run: the current tag is complete.
                    let before = self.now();
                    on_idle();
                    if self.now() != before {
                        continue; // poll whatever the advance woke, or idle again
                    }
                    if !self.tasks.borrow().contains_key(&root_task_id) {
                        return;
                    }
                    self.task_receiver.recv().unwrap()
                }
                Err(TryRecvError::Disconnected) => unreachable!("the runtime holds a sender"),
            };
            let Some(mut task) = self.tasks.borrow_mut().remove(&task_id) else {
                continue; // Waker outlived the task, disregard the wait
            };
            let waker = Waker::from(Arc::new(TaskWaker {
                task_id,
                task_sender: self.task_sender.clone(),
            }));
            let mut context = Context::from_waker(&waker);
            if task.as_mut().poll(&mut context).is_pending() {
                self.tasks.borrow_mut().insert(task_id, task);
            }
        }
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
    fn idle_runs_once_every_woken_task_has_parked() {
        let rt = Rc::new(Runtime::new());
        let log = Rc::new(RefCell::new(Vec::new()));
        for name in ["a", "b"] {
            let log = log.clone();
            rt.spawn(async move {
                log.borrow_mut().push(name);
                YieldOnce(false).await; // re-queued: still runnable, so not idle yet
                log.borrow_mut().push(name);
            });
        }
        let idle_log = log.clone();
        rt.block_on_with_idle(async {}, move || idle_log.borrow_mut().push("idle"));
        assert_eq!(*log.borrow(), vec!["a", "b", "a", "b", "idle"]);
    }

    #[test]
    fn idle_repeats_while_it_advances_time() {
        let rt = Rc::new(Runtime::new());
        let idles = Rc::new(Cell::new(0));
        let (clock, count) = (rt.clone(), idles.clone());
        rt.block_on_with_idle(async {}, move || {
            count.set(count.get() + 1);
            if clock.now() < Time::new(2) {
                clock.advance_to(clock.now() + Time::new(1)); // something may be due at the new tag
            }
        });
        assert_eq!(idles.get(), 3); // advanced twice, then one quiet idle ends the run
        assert_eq!(rt.now(), Time::new(2));
    }

    #[test]
    fn clock_starts_at_zero_and_jumps() {
        let rt = Runtime::new();
        assert_eq!(rt.now(), Time::ZERO);
        rt.advance_to(Time::new(10));
        assert_eq!(rt.now(), Time::new(10));
    }

    #[test]
    #[should_panic(expected = "Cannot advance")]
    fn clock_is_monotonic() {
        let rt = Runtime::new();
        rt.advance_to(Time::new(10));
        rt.advance_to(Time::new(3));
    }

    #[test]
    #[should_panic(expected = "Cannot advance")]
    fn clock_rejects_the_current_tag() {
        let rt = Runtime::new();
        rt.advance_to(Time::new(10));
        rt.advance_to(Time::new(10)); // strict: re-announcing `now` is a bug
    }
}
