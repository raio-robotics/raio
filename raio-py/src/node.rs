//! The node that runs on this thread, and what its tasks ask of it: the clock and shutdown.

use std::cell::RefCell;
use std::rc::Rc;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use raio::app::{Node, OutputPort};
use raio::time::Time;

use crate::awaitable::Awaitable;
use crate::ports::{Port, Source};
use crate::serializer::PyValue;
use crate::time::PyTime;

/// The node that runs on this thread: its clock, its ports and the error that stopped it.
pub(crate) struct RunningNode {
    pub(crate) node: Node,
    pub(crate) ports: Vec<Port>,
    pub(crate) failure: RefCell<Option<PyErr>>,
}

thread_local! {
    /// Set when the node starts, so that the functions Python calls can find it.
    pub(crate) static RUNNING: RefCell<Option<Rc<RunningNode>>> = const { RefCell::new(None) };
}

/// Return the node that runs on this thread.
pub(crate) fn running() -> PyResult<Rc<RunningNode>> {
    RUNNING
        .with_borrow(|running| running.clone())
        .ok_or_else(|| PyRuntimeError::new_err("no raio node is running in this process"))
}

impl RunningNode {
    /// Return the input or the action at this position among the node's ports.
    pub(crate) fn input(&self, index: usize) -> PyResult<&dyn Source> {
        match self.ports.get(index) {
            Some(Port::Input(input)) => Ok(input),
            Some(Port::Action(action)) => Ok(action),
            _ => Err(PyValueError::new_err(format!(
                "port {index} of this node is not an input"
            ))),
        }
    }

    /// Return the output at this position among the node's ports.
    pub(crate) fn output(&self, index: usize) -> PyResult<&OutputPort<PyValue>> {
        match self.ports.get(index) {
            Some(Port::Output(output)) => Ok(output),
            _ => Err(PyValueError::new_err(format!(
                "port {index} of this node is not an output"
            ))),
        }
    }

    /// Keep the first error and stop the node at once.
    pub(crate) fn fail(&self, error: PyErr) {
        self.failure.borrow_mut().get_or_insert(error);
        self.node.abort();
    }
}

/// The current logical timestamp of this node.
#[pyfunction]
fn now() -> PyResult<PyTime> {
    Ok(PyTime(running()?.node.now()))
}

/// The wall-clock time since the node started.
#[pyfunction]
fn physical_now() -> PyResult<PyTime> {
    Ok(PyTime(running()?.node.physical_now()))
}

/// True if the current timestamp was caused by an event from outside the node.
#[pyfunction]
fn is_physical() -> PyResult<bool> {
    Ok(running()?.node.is_physical())
}

/// Wait for a logical duration and return the new timestamp.
#[pyfunction]
fn wait(duration: &PyTime) -> PyResult<Awaitable> {
    let (running, duration) = (running()?, duration.0);
    if duration <= Time::ZERO {
        return Err(PyValueError::new_err(
            "wait needs a positive Time, such as 500 * MS",
        ));
    }
    Ok(Awaitable::new(async move {
        let timestamp = running.node.wait(duration).await;
        Python::attach(|py| Ok(Py::new(py, PyTime(timestamp))?.into_any()))
    }))
}

/// Ask every node of the app to shut down a microstep after the latest of their current timestamps.
#[pyfunction]
fn request_shutdown() -> PyResult<()> {
    running()?.node.request_shutdown();
    Ok(())
}

/// Ask every node of the app to shut down `delay` after the latest of their current timestamps.
#[pyfunction]
fn request_shutdown_after(delay: &PyTime) -> PyResult<()> {
    let (running, delay) = (running()?, delay.0);
    if delay <= Time::ZERO {
        return Err(PyValueError::new_err(
            "request_shutdown_after needs a positive Time, such as 500 * MS",
        ));
    }
    running.node.request_shutdown_after(delay);
    Ok(())
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(now, module)?)?;
    module.add_function(wrap_pyfunction!(physical_now, module)?)?;
    module.add_function(wrap_pyfunction!(is_physical, module)?)?;
    module.add_function(wrap_pyfunction!(wait, module)?)?;
    module.add_function(wrap_pyfunction!(request_shutdown, module)?)?;
    module.add_function(wrap_pyfunction!(request_shutdown_after, module)?)?;
    Ok(())
}
