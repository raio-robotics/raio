use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::poll_fn;
use std::rc::Rc;
use std::task::{Poll, Waker};

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use raio::app::{
    Assignment, Builder, ExecutionMode, InputPort, Node, Start, create_node, run_node_on,
};
use raio::coordinator::Link;
use raio::runtime::{Runtime, TaskId, WakeKind};
use raio::time::Time;

use crate::awaitable::{Awaitable, PyFuture, to_future};
use crate::ports::{Closed, Custom, Port, PyValue, SENDERS};
use crate::time::PyTime;

/// The node that runs on this thread: its clock, its ports and the error that stopped it.
pub(crate) struct Running {
    pub(crate) node: Node,
    pub(crate) ports: Vec<Port>,
    failure: RefCell<Option<PyErr>>,
}

thread_local! {
    static RUNNING: RefCell<Option<Rc<Running>>> = const { RefCell::new(None) };
}

/// Return the node that runs on this thread.
pub(crate) fn running() -> PyResult<Rc<Running>> {
    RUNNING
        .with_borrow(|running| running.clone())
        .ok_or_else(|| PyRuntimeError::new_err("no raio node is running in this process"))
}

impl Running {
    /// Return the input at this position among the node's ports; an action has one too.
    pub(crate) fn input(&self, index: usize) -> PyResult<&InputPort<PyValue>> {
        match self.ports.get(index) {
            Some(Port::Input(input)) => Ok(input),
            Some(Port::Action(action)) => Ok(action.port()),
            _ => Err(PyValueError::new_err(format!(
                "port {index} of this node is not an input"
            ))),
        }
    }

    /// Keep the first error and stop the node at once.
    fn fail(&self, error: PyErr) {
        self.failure.borrow_mut().get_or_insert(error);
        self.node.abort();
    }
}

/// Run one task to its end and return its result; an error other than `Closed` fails the node.
async fn run_task(running: Rc<Running>, task: PyFuture) -> Option<Py<PyAny>> {
    let outcome = task.await;
    Python::attach(|py| match outcome {
        Ok(result) => Some(result),
        // A closed port ends a task quietly.
        Err(error) if error.is_instance_of::<Closed>(py) => Some(py.None()),
        Err(error) => {
            running.fail(error);
            None
        }
    })
}

/// A coroutine running as its own task of the node.
#[pyclass(unsendable, module = "raio")]
#[derive(Default)]
struct Task {
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

/// Start a coroutine as another task of this node.
#[pyfunction]
fn spawn(coroutine: &Bound<'_, PyAny>) -> PyResult<Task> {
    let (running, future) = (running()?, to_future(coroutine)?);
    let task = Task::default();
    let (runner, result, joiners) = (running.clone(), task.result.clone(), task.joiners.clone());
    running.node.spawn(async move {
        // A task that failed has stopped the node, so no task is left to wait for it.
        let Some(finished) = run_task(runner, future).await else {
            return;
        };
        *result.borrow_mut() = Some(finished);
        for (_, joiner) in std::mem::take(&mut *joiners.borrow_mut()) {
            joiner.wake();
        }
    });
    Ok(task)
}

/// Carries the runtime's blocking wait across the release of the GIL.
struct Wait<'a>(&'a mut dyn FnMut());

// SAFETY: The wait runs on the thread that created it; only the GIL is released around it.
unsafe impl Send for Wait<'_> {}

/// Release the GIL while the node's thread waits for its next event.
fn release_gil(wait: &mut dyn FnMut()) {
    let wait = Wait(wait);
    Python::attach(|py| {
        py.detach(move || {
            let wait = wait;
            (wait.0)()
        })
    });
}

/// Run one node in this process until it finishes.
///
/// `load` turns the code that the coordinator holds for the node into the kinds of the node's
/// ports, the serializers that its connections chose for them, each with the port's position
/// and its own name, and a function that returns the coroutines of its tasks.
#[pyfunction]
fn run_node(
    py: Python<'_>,
    node_id: usize,
    coordinator_endpoint: String,
    load: Py<PyAny>,
    fast_forward: bool,
) -> PyResult<()> {
    let mut link = Link::open(Assignment {
        node_id,
        coordinator_endpoint,
    });
    let code = link.code.take().ok_or_else(|| {
        PyRuntimeError::new_err(format!("the coordinator holds no code for node {node_id}"))
    })?;
    let loaded = load.call1(py, (PyBytes::new(py, &code),))?;
    type Loaded = (Vec<String>, Vec<(usize, String, Py<PyAny>)>, Py<PyAny>);
    let (kinds, serializers, start): Loaded = loaded.extract(py)?;
    let known = ["input", "output", "action"];
    if let Some(kind) = kinds.iter().find(|kind| !known.contains(&kind.as_str())) {
        return Err(PyValueError::new_err(format!("unknown port kind {kind:?}")));
    }
    // The ports are built in the order Python numbered them.
    let build = move |builder: &mut Builder| {
        if fast_forward {
            builder.execution_mode(ExecutionMode::FastForward);
        }
        let ports = kinds
            .iter()
            .enumerate()
            .map(|(index, kind)| match kind.as_str() {
                "input" => Port::Input(builder.input()),
                "output" => Port::Output(builder.output()),
                _ => {
                    let action = builder.action();
                    // The sender is kept where any thread of this process can reach it.
                    SENDERS.lock().unwrap().insert(index, action.handle());
                    Port::Action(action)
                }
            });
        let ports: Vec<Port> = ports.collect();
        for (index, name, serializer) in serializers {
            let custom = Custom { name, serializer };
            match ports.get(index) {
                // An input has the one serializer of its connection.
                Some(Port::Input(input)) => input.set_serializer(custom),
                // An output keeps one for each choice that its connections make.
                Some(Port::Output(output)) => output.add_serializer(custom),
                _ => panic!("port {index} of this node takes no serializer"),
            }
        }
        Running {
            ports,
            node: builder.node(),
            failure: RefCell::new(None),
        }
    };
    let start: Start<Running> = Box::new(move |running| {
        Box::pin(async move {
            RUNNING.set(Some(running.clone()));
            let tasks = Python::attach(|py| {
                let coroutines = start.call0(py)?.extract::<Vec<Py<PyAny>>>(py)?;
                let tasks = coroutines.iter().map(|task| to_future(task.bind(py)));
                tasks.collect::<PyResult<Vec<_>>>()
            });
            match tasks {
                Ok(tasks) => {
                    for task in tasks {
                        let runner = running.clone();
                        running.node.spawn(async move {
                            run_task(runner, task).await;
                        });
                    }
                }
                Err(error) => running.fail(error),
            }
        })
    });
    Runtime::set_idle_hook(release_gil);
    run_node_on(link, create_node(build, vec![start]));
    SENDERS.lock().unwrap().clear(); // A value sent from now on finds its action closed.
    let failure = RUNNING.take().and_then(|running| running.failure.take());
    failure.map_or(Ok(()), Err)
}

/// The current logical tag of this node.
#[pyfunction]
fn now() -> PyResult<PyTime> {
    Ok(PyTime(running()?.node.now()))
}

/// The wall-clock time since the node started.
#[pyfunction]
fn physical_now() -> PyResult<PyTime> {
    Ok(PyTime(running()?.node.physical_now()))
}

/// True if the current tag was caused by an event from outside the node.
#[pyfunction]
fn is_physical() -> PyResult<bool> {
    Ok(running()?.node.is_physical())
}

/// Wait for a logical duration and return the new tag.
#[pyfunction]
fn sleep(duration: &PyTime) -> PyResult<Awaitable> {
    let (running, duration) = (running()?, duration.0);
    if duration <= Time::ZERO {
        return Err(PyValueError::new_err(
            "sleep needs a positive Time, such as 500 * MS",
        ));
    }
    Ok(Awaitable::new(async move {
        let tag = running.node.wait(duration).await;
        Python::attach(|py| Ok(Py::new(py, PyTime(tag))?.into_any()))
    }))
}

/// Add this module's class and functions to the Python module.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Task>()?;
    module.add_function(wrap_pyfunction!(spawn, module)?)?;
    module.add_function(wrap_pyfunction!(run_node, module)?)?;
    module.add_function(wrap_pyfunction!(now, module)?)?;
    module.add_function(wrap_pyfunction!(physical_now, module)?)?;
    module.add_function(wrap_pyfunction!(is_physical, module)?)?;
    module.add_function(wrap_pyfunction!(sleep, module)?)?;
    Ok(())
}
