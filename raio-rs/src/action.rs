use std::cell::{Ref, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use futures::channel::mpsc::UnboundedSender;

use crate::port::InputPort;
use crate::time::Time;

/// An event source fed from outside the node: any thread holding an [`ActionHandle`]
/// can schedule a value. Each value becomes a physical event, tagged when the node
/// receives it, and is read like an input port.
pub struct Action<T> {
    pub(crate) port: InputPort<T>,
    /// Handed to the first handle; `None` once handles exist or the action has closed.
    pub(crate) initial: RefCell<Option<Arc<UnboundedSender<T>>>>,
    /// The sender every live handle shares.
    pub(crate) live: RefCell<Weak<UnboundedSender<T>>>,
}

impl<T> Action<T> {
    /// A `Clone + Send` producer. Create handles in `start`, not in the constructor:
    /// the constructor also runs in the launcher process. Dropping the last handle
    /// closes the action.
    pub fn handle(&self) -> ActionHandle<T> {
        let live = self.live.borrow().upgrade();
        let sender = match live {
            Some(sender) => sender,
            None => {
                let sender = self
                    .initial
                    .borrow_mut()
                    .take()
                    .expect("action is closed: every handle was dropped");
                *self.live.borrow_mut() = Arc::downgrade(&sender);
                sender
            }
        };
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
    /// Schedule `value` on the node. It is tagged with the node's physical clock when received.
    pub fn send(&self, value: T) {
        if self.sender.unbounded_send(value).is_err() {
            panic!("the action's node has stopped");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::NodeContext;
    use crate::port::Builder;
    use crate::runtime::Runtime;
    use std::rc::Rc;

    fn rig() -> (Rc<Runtime>, Rc<NodeContext>, Builder) {
        let runtime = Rc::new(Runtime::new());
        let ctx = Rc::new(NodeContext::new(runtime.clone()));
        let builder = Builder::new(0, ctx.clone());
        (runtime, ctx, builder)
    }

    #[test]
    fn values_arrive_physically_in_send_order_until_the_last_handle_drops() {
        let (runtime, ctx, mut builder) = rig();
        let action = Rc::new(builder.action::<i64>());
        for task in std::mem::take(&mut builder.action_tasks) {
            runtime.spawn(task);
        }
        assert_eq!(ctx.ent(), Time::FOREVER); // an action is never promised

        let early = action.handle();
        let producer = action.handle(); // shares the channel
        drop(early); // not the last handle: the action stays open
        let worker = std::thread::spawn(move || {
            for value in 1..=3 {
                producer.send(value);
            }
        }); // `producer` drops with the thread: the action closes

        let seen = Rc::new(RefCell::new(Vec::new()));
        let (out, task_ctx, task_action) = (seen.clone(), ctx.clone(), action.clone());
        let idle_ctx = ctx.clone();
        runtime.block_on_with_idle(
            async move {
                while let Some((value, tag)) = task_action.wait().await {
                    out.borrow_mut().push((*value, tag, task_ctx.physical()));
                    assert_eq!(task_ctx.ent(), Time::FOREVER);
                }
            },
            move || idle_ctx.try_advance(),
        );
        worker.join().unwrap();

        let seen = seen.borrow();
        let values: Vec<_> = seen.iter().map(|(value, _, _)| *value).collect();
        assert_eq!(values, vec![1, 2, 3]);
        assert!(seen.iter().all(|(_, _, physical)| *physical));
        assert!(seen[0].1 > Time::ZERO);
        assert!(seen.windows(2).all(|pair| pair[0].1 < pair[1].1)); // one tag per value
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
