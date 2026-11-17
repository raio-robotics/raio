use core::task;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::task::{Context, Wake, Waker};

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
}

impl Runtime {
    pub fn new() -> Self {
        let (task_sender, task_receiver) = channel();
        Self {
            task_sender,
            task_receiver,
            tasks: RefCell::new(HashMap::new()),
            next_task_id: Cell::new(0),
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
