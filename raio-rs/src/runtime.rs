use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Wake, Waker};

struct Task {
    future: Mutex<Pin<Box<dyn Future<Output = ()> + Send>>>,
    // Send: future is sent between threads, so it must be Send.
    // Mutex: (1) poll needs &mut but Arc only gives & — runtime-checked exclusivity;
    //        (2) makes Task Sync, required to share the Arc across threads (Waker::from bound).
    //        (3) The mutex is only locked in the executor thread, so there is no contention.
    task_sender: std::sync::mpsc::Sender<Arc<Task>>,
}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        self.task_sender.send(self.clone()).unwrap();
    }
}

pub struct Runtime {
    ready_queue: std::sync::mpsc::Receiver<Arc<Task>>,
    task_sender: std::sync::mpsc::Sender<Arc<Task>>,
}

impl Runtime {
    pub fn new() -> Self {
        let (task_sender, ready_queue) = std::sync::mpsc::channel();
        Self {
            ready_queue,
            task_sender: task_sender.clone(),
        }
    }
    pub fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        // 'static: the task outlives every scope, so the future may not borrow from any.
        let future = Box::pin(future);
        let task = Task {
            future: Mutex::new(future),
            task_sender: self.task_sender.clone(),
        };
        self.task_sender.send(Arc::new(task)).unwrap();
    }
    pub fn block_on(&self, future: impl Future<Output = ()> + Send + 'static) {
        let root_task = Arc::new(Task {
            future: Mutex::new(Box::pin(future)),
            task_sender: self.task_sender.clone(),
        });
        self.task_sender.send(root_task.clone()).unwrap();
        while let Ok(task) = self.ready_queue.recv() {
            let waker = Waker::from(task.clone());
            let mut context = Context::from_waker(&waker);
            let ret = task.future.lock().unwrap().as_mut().poll(&mut context);
            if ret.is_ready() && Arc::ptr_eq(&task, &root_task) {
                break;
            }
        }
    }
}
