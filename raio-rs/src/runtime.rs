use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::task::{Context, Wake, Waker};

use crate::time::Time;

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
    wakers: RefCell<Vec<Waker>>,
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
            wakers: RefCell::new(Vec::new()),
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

    pub fn register_waker(&self, waker: Waker) {
        self.wakers.borrow_mut().push(waker.clone());
    }

    pub fn wake_wakers(&self) {
        for waker in self.wakers.borrow_mut().drain(..) {
            waker.wake();
        }
    }

    pub fn spawn(&self, future: impl Future<Output = ()> + 'static) -> TaskId {
        let task_id = self.next_task_id.get();
        self.next_task_id.set(task_id + 1);
        self.tasks.borrow_mut().insert(task_id, Box::pin(future));
        self.task_sender.send(task_id).unwrap();
        task_id
    }

    pub fn block_on(&self, future: impl Future<Output = ()> + 'static) {
        let root_task_id = self.spawn(future);
        while let Ok(task_id) = self.task_receiver.recv() {
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
            } else if task_id == root_task_id {
                return;
            } else {
                continue;
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

    #[test]
    fn wake_reschedules_a_suspended_task() {
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
        Runtime::new().block_on(async { YieldOnce(false).await });
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
