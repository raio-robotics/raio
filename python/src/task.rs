//! The tasks of a node: each drives one Python coroutine on the node's runtime.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::poll_fn;
use std::rc::Rc;
use std::task::{Poll, Waker};

use pyo3::prelude::*;
use raio::runtime::{Runtime, TaskId, WakeKind};

use crate::awaitable::{Awaitable, PyFuture, to_future};
use crate::node::{RunningNode, running};
use crate::ports::Closed;

/// A coroutine running as its own task of the node.
#[pyclass(unsendable, module = "raio")]
#[derive(Default)]
pub(crate) struct Task {
    result: Rc<RefCell<Option<Py<PyAny>>>>,
    /// The tasks that wait for the result.
    joiners: Rc<RefCell<BTreeMap<TaskId, Waker>>>,
}

#[pymethods]
impl Task {
    /// True if the task has finished.
    fn done(&self) -> bool {
        self.result.borrow().is_some()
    }

    /// Wait for the task and return its result.
    fn __await__(&self) -> Awaitable {
        let (result, joiners) = (self.result.clone(), self.joiners.clone());
        Awaitable::new(poll_fn(move |cx| {
            if let Some(result) = &*result.borrow() {
                return Poll::Ready(Python::attach(|py| Ok(result.clone_ref(py))));
            }
            let waker = Runtime::waker(cx, WakeKind::Resume);
            joiners
                .borrow_mut()
                .insert(Runtime::current_task(cx), waker);
            Poll::Pending
        }))
    }
}

/// Run a future as a task of the node. An error other than `Closed` fails the node.
pub(crate) fn spawn_task(running: &Rc<RunningNode>, future: PyFuture) -> Task {
    let task = Task::default();
    let (node, result, joiners) = (running.clone(), task.result.clone(), task.joiners.clone());
    running.node.spawn(async move {
        let outcome = future.await;
        let finished = Python::attach(|py| match outcome {
            Ok(result) => Some(result),
            // A closed port ends a task quietly.
            Err(error) if error.is_instance_of::<Closed>(py) => Some(py.None()),
            Err(error) => {
                node.fail(error);
                None
            }
        });
        // A task that failed has stopped the node, so no task is left to wait for it.
        let Some(finished) = finished else {
            return;
        };
        *result.borrow_mut() = Some(finished);
        for (_, joiner) in joiners.take() {
            joiner.wake();
        }
    });
    task
}

/// Start a coroutine as another task of this node.
#[pyfunction]
fn spawn(coroutine: &Bound<'_, PyAny>) -> PyResult<Task> {
    Ok(spawn_task(&running()?, to_future(coroutine)?))
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Task>()?;
    module.add_function(wrap_pyfunction!(spawn, module)?)?;
    Ok(())
}
