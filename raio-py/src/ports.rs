use std::rc::Rc;

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError, PyStopAsyncIteration, PyTypeError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyBytes;
use raio::app::{InputPort, OutputPort};
use raio::serializer::{Payload, Serializer};
use raio::time::Time;

use crate::awaitable::Awaitable;
use crate::node::running;
use crate::time::PyTime;

create_exception!(
    raio,
    Closed,
    PyException,
    "Raised when awaiting a port that will deliver no more values."
);

static DUMPS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static LOADS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

/// A Python value carried by a port.
pub(crate) struct PyValue(Py<PyAny>);

/// Serializes port values with Python's pickle.
struct Pickle;

impl Serializer<PyValue> for Pickle {
    fn name(&self) -> &str {
        "pickle"
    }

    fn serialize(&self, value: &PyValue) -> Vec<u8> {
        Python::attach(|py| {
            let dumps = DUMPS.import(py, "pickle", "dumps").unwrap();
            let data = dumps
                .call1((value.0.bind(py), -1))
                .unwrap_or_else(|error| panic!("cannot pickle a port value: {error}"));
            data.cast::<PyBytes>().unwrap().as_bytes().to_vec()
        })
    }

    fn deserialize(&self, bytes: &[u8]) -> PyValue {
        Python::attach(|py| {
            let loads = LOADS.import(py, "pickle", "loads").unwrap();
            let value = loads
                .call1((PyBytes::new(py, bytes),))
                .unwrap_or_else(|error| panic!("cannot unpickle a port value: {error}"));
            PyValue(value.unbind())
        })
    }
}

impl Payload for PyValue {
    fn default_serializer() -> Rc<dyn Serializer<Self>> {
        Rc::new(Pickle)
    }
}

/// One port of a node, as raio-rs built it.
pub(crate) enum Port {
    Input(InputPort<PyValue>),
    Output(OutputPort<PyValue>),
}

fn not_an(kind: &str, index: usize) -> PyErr {
    PyTypeError::new_err(format!("port {index} of this node is not an {kind}"))
}

/// Set the value an output sends when the current tag completes.
#[pyfunction]
fn output_set(index: usize, value: Py<PyAny>) -> PyResult<()> {
    let running = running()?;
    let Some(Port::Output(output)) = running.ports.get(index) else {
        return Err(not_an("output", index));
    };
    if running.node.tag_complete() {
        return Err(PyRuntimeError::new_err(format!(
            "output set at {}, a tag the node already completed; \
             tag 0 completes at startup, so wait first",
            running.node.now()
        )));
    }
    output.set(PyValue(value));
    Ok(())
}

/// Clear the value set on an output at the current tag.
#[pyfunction]
fn output_reset(index: usize) -> PyResult<()> {
    let running = running()?;
    let Some(Port::Output(output)) = running.ports.get(index) else {
        return Err(not_an("output", index));
    };
    output.reset();
    Ok(())
}

/// The latest value of an input, or None before the first one.
#[pyfunction]
fn input_value(py: Python<'_>, index: usize) -> PyResult<Option<Py<PyAny>>> {
    let running = running()?;
    let latest = running.input(index)?.latest();
    Ok(latest.map(|(value, _)| value.0.clone_ref(py)))
}

/// The tag of the latest value of an input, or NEVER before the first one.
#[pyfunction]
fn input_time(index: usize) -> PyResult<PyTime> {
    let running = running()?;
    let latest = running.input(index)?.latest();
    Ok(PyTime(latest.map_or(Time::NEVER, |(_, tag)| tag)))
}

/// True if a value arrived on an input at the current tag.
#[pyfunction]
fn input_present(index: usize) -> PyResult<bool> {
    Ok(running()?.input(index)?.get().is_some())
}

/// True if no more values will arrive on an input.
#[pyfunction]
fn input_closed(index: usize) -> PyResult<bool> {
    Ok(running()?.input(index)?.closed())
}

/// Wait for the next value of an input after the current tag.
#[pyfunction]
fn input_wait(index: usize, label: String, iterating: bool) -> PyResult<Awaitable> {
    let running = running()?;
    running.input(index)?;
    Ok(Awaitable::new(async move {
        let received = running.input(index)?.wait().await;
        Python::attach(|py| match received {
            Some((value, _)) => Ok(value.0.clone_ref(py)),
            None if iterating => Err(PyStopAsyncIteration::new_err(())),
            None => Err(Closed::new_err(format!("{label} is closed"))),
        })
    }))
}

/// Add this module's exception and functions to the Python module.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("Closed", module.py().get_type::<Closed>())?;
    module.add_function(wrap_pyfunction!(output_set, module)?)?;
    module.add_function(wrap_pyfunction!(output_reset, module)?)?;
    module.add_function(wrap_pyfunction!(input_value, module)?)?;
    module.add_function(wrap_pyfunction!(input_time, module)?)?;
    module.add_function(wrap_pyfunction!(input_present, module)?)?;
    module.add_function(wrap_pyfunction!(input_closed, module)?)?;
    module.add_function(wrap_pyfunction!(input_wait, module)?)?;
    Ok(())
}
