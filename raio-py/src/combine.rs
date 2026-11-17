use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use crate::awaitable::{Awaitable, Outcome, PyFuture, to_future};

/// Finishes with every result once all futures have finished, or with the first error.
struct Join {
    futures: Vec<Option<PyFuture>>,
    results: Vec<Option<Py<PyAny>>>,
}

impl Future for Join {
    type Output = Outcome;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Outcome> {
        let this = &mut *self;
        for (slot, result) in this.futures.iter_mut().zip(&mut this.results) {
            let Some(future) = slot else { continue };
            match future.as_mut().poll(cx) {
                Poll::Pending => {}
                Poll::Ready(Ok(value)) => {
                    *result = Some(value);
                    *slot = None;
                }
                Poll::Ready(Err(error)) => {
                    this.futures.clear(); // Dropping the others cancels them.
                    return Poll::Ready(Err(error));
                }
            }
        }
        if this.futures.iter().any(Option::is_some) {
            return Poll::Pending;
        }
        Python::attach(|py| {
            let results = this.results.drain(..).map(Option::unwrap);
            Poll::Ready(PyTuple::new(py, results).map(|results| results.into_any().unbind()))
        })
    }
}

/// Finishes with every future that has finished as soon as one has, and drops the others.
struct Select {
    keys: Vec<Py<PyAny>>,
    futures: Vec<PyFuture>,
}

impl Future for Select {
    type Output = Outcome;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Outcome> {
        let this = &mut *self;
        // Every future is polled, so that the first to finish cannot hide another at its tag.
        let outcomes: Vec<_> = this
            .futures
            .iter_mut()
            .map(|future| future.as_mut().poll(cx))
            .collect();
        if outcomes.iter().all(Poll::is_pending) {
            return Poll::Pending;
        }
        this.futures.clear(); // Dropping the others cancels them.
        Python::attach(|py| {
            let finished = PyDict::new(py);
            for (key, outcome) in this.keys.iter().zip(outcomes) {
                if let Poll::Ready(outcome) = outcome {
                    finished.set_item(key, outcome?)?;
                }
            }
            Ok(finished.into_any().unbind())
        })
        .into()
    }
}

/// Await everything concurrently and return the results in order.
#[pyfunction]
#[pyo3(signature = (*awaitables))]
fn gather(awaitables: &Bound<'_, PyTuple>) -> PyResult<Awaitable> {
    let futures = awaitables
        .iter()
        .map(|awaitable| to_future(&awaitable).map(Some))
        .collect::<PyResult<Vec<_>>>()?;
    let results = futures.iter().map(|_| None).collect();
    Ok(Awaitable::new(Join { futures, results }))
}

/// Wait for the first awaitable to finish and drop the others.
#[pyfunction]
#[pyo3(signature = (*awaitables, **named))]
fn select(
    py: Python<'_>,
    awaitables: &Bound<'_, PyTuple>,
    named: Option<&Bound<'_, PyDict>>,
) -> PyResult<Awaitable> {
    let mut keys = Vec::new();
    let mut futures = Vec::new();
    // An awaitable is known by its position, or by its name if it has one.
    for (position, awaitable) in awaitables.iter().enumerate() {
        keys.push(position.into_pyobject(py)?.into_any().unbind());
        futures.push(to_future(&awaitable)?);
    }
    for (name, awaitable) in named.into_iter().flatten() {
        keys.push(name.unbind());
        futures.push(to_future(&awaitable)?);
    }
    if futures.is_empty() {
        return Err(PyValueError::new_err("select needs at least one awaitable"));
    }
    Ok(Awaitable::new(Select { keys, futures }))
}

/// Add this module's functions to the Python module.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(gather, module)?)?;
    module.add_function(wrap_pyfunction!(select, module)?)?;
    Ok(())
}
