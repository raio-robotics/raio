use std::cell::RefCell;
use std::rc::Rc;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use raio::app::{
    Assignment, Builder, ExecutionMode, InputPort, Node, Start, create_node, run_node_on,
};
use raio::coordinator::Link;
use raio::runtime::Runtime;
use raio::time::Time;

use crate::awaitable::{Awaitable, Coroutine};
use crate::ports::{Closed, Port, PyValue};
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
    /// Return the input at this position among the node's ports.
    pub(crate) fn input(&self, index: usize) -> PyResult<&InputPort<PyValue>> {
        match self.ports.get(index) {
            Some(Port::Input(input)) => Ok(input),
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

/// Run one task to its end; an error other than `Closed` fails the node.
async fn run_task(running: Rc<Running>, coroutine: Py<PyAny>) {
    let outcome = Coroutine::new(coroutine).await;
    Python::attach(|py| match outcome {
        Ok(_) => {}
        Err(error) if error.is_instance_of::<Closed>(py) => {} // A closed port ends a task quietly.
        Err(error) => running.fail(error),
    })
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
/// ports and a function that returns the coroutines of its tasks.
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
    let (kinds, start): (Vec<String>, Py<PyAny>) = loaded.extract(py)?;
    if let Some(kind) = kinds
        .iter()
        .find(|kind| *kind != "input" && *kind != "output")
    {
        return Err(PyValueError::new_err(format!("unknown port kind {kind:?}")));
    }
    // The ports are built in the order Python numbered them.
    let build = move |builder: &mut Builder| {
        if fast_forward {
            builder.execution_mode(ExecutionMode::FastForward);
        }
        let ports = kinds.iter().map(|kind| match kind.as_str() {
            "input" => Port::Input(builder.input()),
            _ => Port::Output(builder.output()),
        });
        Running {
            ports: ports.collect(),
            node: builder.node(),
            failure: RefCell::new(None),
        }
    };
    let start: Start<Running> = Box::new(move |running| {
        Box::pin(async move {
            RUNNING.set(Some(running.clone()));
            let tasks = Python::attach(|py| start.call0(py)?.extract::<Vec<Py<PyAny>>>(py));
            match tasks {
                Ok(tasks) => {
                    for task in tasks {
                        running.node.spawn(run_task(running.clone(), task));
                    }
                }
                Err(error) => running.fail(error),
            }
        })
    });
    Runtime::set_idle_hook(release_gil);
    run_node_on(link, create_node(build, vec![start]));
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

/// Add this module's functions to the Python module.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(run_node, module)?)?;
    module.add_function(wrap_pyfunction!(now, module)?)?;
    module.add_function(wrap_pyfunction!(physical_now, module)?)?;
    module.add_function(wrap_pyfunction!(is_physical, module)?)?;
    module.add_function(wrap_pyfunction!(sleep, module)?)?;
    Ok(())
}
