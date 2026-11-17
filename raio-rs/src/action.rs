use std::cell::{Ref, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use futures::channel::mpsc::UnboundedSender;

use crate::port::InputPort;
use crate::time::Time;

/// Receives values from other threads as physical events.
pub struct Action<T> {
    pub(crate) port: InputPort<T>,
    pub(crate) handles: RefCell<Handles<T>>,
}

/// Transfers sender ownership from the action to its handles when the first handle is created.
pub(crate) enum Handles<T> {
    Unused(Arc<UnboundedSender<T>>),
    Live(Weak<UnboundedSender<T>>),
}

impl<T> Action<T> {
    /// Create a producer handle; dropping the last handle closes the action.
    pub fn handle(&self) -> ActionHandle<T> {
        let mut handles = self.handles.borrow_mut();
        let sender = match &*handles {
            Handles::Unused(sender) => sender.clone(),
            Handles::Live(sender) => sender
                .upgrade()
                .expect("action is closed: every handle was dropped"),
        };
        *handles = Handles::Live(Arc::downgrade(&sender));
        ActionHandle { sender }
    }

    pub fn get(&self) -> Option<Ref<'_, T>> {
        self.port.get()
    }

    pub fn latest(&self) -> Option<(Ref<'_, T>, Time)> {
        self.port.latest()
    }

    pub fn wait(&self) -> impl Future<Output = Option<(Ref<'_, T>, Time)>> {
        self.port.wait()
    }

    pub fn pin(&self) -> Pin<Box<impl Future<Output = Option<(Ref<'_, T>, Time)>>>> {
        self.port.pin()
    }

    pub fn trigger(&self) -> Pin<Box<dyn Future<Output = bool> + '_>> {
        self.port.trigger()
    }
}

pub struct ActionHandle<T> {
    sender: Arc<UnboundedSender<T>>,
}

impl<T> Clone for ActionHandle<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<T> ActionHandle<T> {
    /// Send a value; the receiving node assigns its physical tag.
    pub fn send(&self, value: T) {
        if self.sender.unbounded_send(value).is_err() {
            panic!("the action's node has stopped");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::NodeScheduler;
    use crate::port::Builder;
    use crate::runtime::Runtime;
    use std::rc::Rc;

    fn rig() -> (Rc<Runtime>, Rc<NodeScheduler>, Builder) {
        let runtime = Rc::new(Runtime::new());
        let scheduler = Rc::new(NodeScheduler::new(runtime.clone()));
        let builder = Builder::new(0, scheduler.clone());
        (runtime, scheduler, builder)
    }

    #[test]
    fn values_arrive_physically_in_send_order_until_the_last_handle_drops() {
        let (runtime, scheduler, mut builder) = rig();
        let action = Rc::new(builder.action::<i64>());
        for task in std::mem::take(&mut builder.tasks) {
            runtime.spawn_internal(task);
        }
        // Actions produce no logical events, so they do not limit ENT.
        assert_eq!(scheduler.ent(), Time::FOREVER);

        let early = action.handle();
        let producer = action.handle(); // Both handles send to the same action.
        drop(early); // The remaining handle keeps the action open.
        let worker = std::thread::spawn(move || {
            for value in 1..=3 {
                producer.send(value);
            }
        }); // Exiting the thread drops the last handle and closes the action.

        let seen = Rc::new(RefCell::new(Vec::new()));
        let (out, task_scheduler, task_action) = (seen.clone(), scheduler.clone(), action.clone());
        scheduler.run_user(async move {
            while let Some((value, tag)) = task_action.wait().await {
                out.borrow_mut()
                    .push((*value, tag, task_scheduler.is_physical()));
                assert_eq!(task_scheduler.ent(), Time::FOREVER);
            }
        });
        worker.join().unwrap();

        let seen = seen.borrow();
        let values: Vec<_> = seen.iter().map(|(value, _, _)| *value).collect();
        assert_eq!(values, vec![1, 2, 3]);
        assert!(seen.iter().all(|(_, _, physical)| *physical));
        assert!(seen[0].1 > Time::ZERO);
        // Each value receives a strictly increasing tag.
        assert!(seen.windows(2).all(|pair| pair[0].1 < pair[1].1));
    }

    #[test]
    #[should_panic(expected = "action is closed")]
    fn a_handle_after_the_last_drop_is_a_bug() {
        let (_runtime, _ctx, mut builder) = rig();
        let action = builder.action::<i64>();
        drop(action.handle());
        action.handle();
    }
}
