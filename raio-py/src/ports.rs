//! A node's ports as Python reaches them: by their position among the ports of the node that
//! runs on this thread.

use std::cell::Ref;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError, PyStopAsyncIteration};
use pyo3::prelude::*;
use raio::app::{Action, ActionHandle, InputPort, OutputPort, Trigger};
use raio::diagram::PortKind;
use raio::time::Time;

use crate::awaitable::Awaitable;
use crate::node::running;
use crate::serializer::PyValue;
use crate::time::PyTime;

create_exception!(
    raio,
    Closed,
    PyException,
    "Raised when awaiting a port that will deliver no more values."
);

/// One port of a node, as raio-rs built it.
pub(crate) enum Port {
    Input(InputPort<PyValue>),
    Output(OutputPort<PyValue>),
    Action(Action<PyValue>),
}

/// Return the kind of a port from the name that the Python half has for it.
pub(crate) fn port_kind(name: &str) -> PortKind {
    match name {
        "input" => PortKind::Input,
        "output" => PortKind::Output,
        "action" => PortKind::Action,
        name => panic!("unknown port kind {name:?}"),
    }
}

/// The latest value of an input or an action, and its timestamp.
type Latest<'a> = Option<(Ref<'a, PyValue>, Time)>;

/// What Python reads values from: an input port or an action.
pub(crate) trait Source {
    fn get(&self) -> Option<Ref<'_, PyValue>>;
    fn latest(&self) -> Latest<'_>;
    fn closed(&self) -> bool;
    fn wait(&self) -> Pin<Box<dyn Future<Output = Latest<'_>> + '_>>;
    fn trigger(&self) -> Trigger<'_>;
}

/// Read a raio type as a `Source` through the methods of the same names that it has.
macro_rules! source {
    ($type:ty) => {
        impl Source for $type {
            fn get(&self) -> Option<Ref<'_, PyValue>> {
                <$type>::get(self)
            }

            fn latest(&self) -> Latest<'_> {
                <$type>::latest(self)
            }

            fn closed(&self) -> bool {
                <$type>::closed(self)
            }

            fn wait(&self) -> Pin<Box<dyn Future<Output = Latest<'_>> + '_>> {
                Box::pin(<$type>::wait(self))
            }

            fn trigger(&self) -> Trigger<'_> {
                <$type>::trigger(self)
            }
        }
    };
}

source!(InputPort<PyValue>);
source!(Action<PyValue>);

/// The sending end of each open action of this process's node, by the action's position.
/// Any thread may send to an action, so the senders are not kept with the node's thread.
pub(crate) static ACTION_SENDERS: Mutex<BTreeMap<usize, ActionHandle<PyValue>>> =
    Mutex::new(BTreeMap::new());

/// Set the value an output sends when the current timestamp completes.
#[pyfunction]
fn output_set(index: usize, value: Py<PyAny>) -> PyResult<()> {
    let running = running()?;
    let output = running.output(index)?;
    if running.node.timestamp_complete() {
        return Err(PyRuntimeError::new_err(format!(
            "output set at {}, a timestamp the node already completed; \
             timestamp 0 completes at startup, so wait first",
            running.node.now()
        )));
    }
    output.set(PyValue(value));
    Ok(())
}

/// Clear the value set on an output at the current timestamp.
#[pyfunction]
fn output_reset(index: usize) -> PyResult<()> {
    running()?.output(index)?.reset();
    Ok(())
}

/// The latest value of an input, or None before the first one.
#[pyfunction]
fn input_value(py: Python<'_>, index: usize) -> PyResult<Option<Py<PyAny>>> {
    let running = running()?;
    let latest = running.input(index)?.latest();
    Ok(latest.map(|(value, _)| value.0.clone_ref(py)))
}

/// The timestamp of the latest value of an input, or NEVER before the first one.
#[pyfunction]
fn input_time(index: usize) -> PyResult<PyTime> {
    let running = running()?;
    let latest = running.input(index)?.latest();
    Ok(PyTime(
        latest.map_or(Time::NEVER, |(_, timestamp)| timestamp),
    ))
}

/// True if a value arrived on an input at the current timestamp.
#[pyfunction]
fn input_present(index: usize) -> PyResult<bool> {
    Ok(running()?.input(index)?.get().is_some())
}

/// True if no more values will arrive on an input.
#[pyfunction]
fn input_closed(index: usize) -> PyResult<bool> {
    Ok(running()?.input(index)?.closed())
}

/// Wait for the next value of an input after the current timestamp.
#[pyfunction]
fn input_wait(index: usize, label: String, iterating: bool) -> PyResult<Awaitable> {
    let running = running()?;
    Ok(Awaitable::new(async move {
        let received = running.input(index)?.wait().await;
        Python::attach(|py| match received {
            Some((value, _)) => Ok(value.0.clone_ref(py)),
            None if iterating => Err(PyStopAsyncIteration::new_err(())),
            None => Err(Closed::new_err(format!("{label} is closed"))),
        })
    }))
}

/// Wait for the next timestamp at which one of these inputs has a value; return False once all of
/// them have closed.
#[pyfunction]
fn next_event(indices: Vec<usize>) -> PyResult<Awaitable> {
    let running = running()?;
    Ok(Awaitable::new(async move {
        let triggers = indices
            .iter()
            .map(|index| Ok(running.input(*index)?.trigger()));
        let fired = raio::app::next_event(triggers.collect::<PyResult<_>>()?).await;
        Python::attach(|py| Ok(fired.into_pyobject(py)?.to_owned().into_any().unbind()))
    }))
}

/// Deliver a value to an action at a fresh physical timestamp; any thread may call this.
#[pyfunction]
fn action_send(index: usize, label: String, value: Py<PyAny>) -> PyResult<()> {
    match ACTION_SENDERS.lock().unwrap().get(&index) {
        Some(sender) => {
            sender.send(PyValue(value));
            Ok(())
        }
        None => Err(PyRuntimeError::new_err(format!("{label} is closed"))),
    }
}

/// End an action after the values already sent; any thread may call this.
#[pyfunction]
fn action_close(index: usize) {
    ACTION_SENDERS.lock().unwrap().remove(&index); // Dropping the only sender closes the action.
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("Closed", module.py().get_type::<Closed>())?;
    module.add_function(wrap_pyfunction!(output_set, module)?)?;
    module.add_function(wrap_pyfunction!(output_reset, module)?)?;
    module.add_function(wrap_pyfunction!(input_value, module)?)?;
    module.add_function(wrap_pyfunction!(input_time, module)?)?;
    module.add_function(wrap_pyfunction!(input_present, module)?)?;
    module.add_function(wrap_pyfunction!(input_closed, module)?)?;
    module.add_function(wrap_pyfunction!(input_wait, module)?)?;
    module.add_function(wrap_pyfunction!(next_event, module)?)?;
    module.add_function(wrap_pyfunction!(action_send, module)?)?;
    module.add_function(wrap_pyfunction!(action_close, module)?)?;
    Ok(())
}
