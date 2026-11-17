use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use pyo3::exceptions::{PyRuntimeError, PyStopIteration, PyTypeError};
use pyo3::prelude::*;

/// The result of a finished future: a value, or the exception to raise.
pub(crate) type Outcome = PyResult<Py<PyAny>>;

/// A future of the node's runtime whose result goes back into Python.
pub(crate) type PyFuture = Pin<Box<dyn Future<Output = Outcome>>>;

/// Hands a future of the node's runtime to the task whose coroutine awaits it.
#[pyclass(unsendable, module = "raio")]
pub(crate) struct Awaitable {
    future: Option<PyFuture>,
    outcome: Option<Outcome>,
    yielded: bool,
}

impl Awaitable {
    pub(crate) fn new(future: impl Future<Output = Outcome> + 'static) -> Self {
        Self {
            future: Some(Box::pin(future)),
            outcome: None,
            yielded: false,
        }
    }
}

#[pymethods]
impl Awaitable {
    fn __await__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Yield this awaitable to the task once, then finish with the outcome the task stored.
    fn __next__(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        let mut this = slf.borrow_mut();
        if !this.yielded {
            this.yielded = true;
            return Ok(slf.clone().into_any().unbind());
        }
        match this.outcome.take() {
            Some(Ok(value)) => Err(PyStopIteration::new_err((value,))),
            Some(Err(error)) => Err(error),
            None => Err(PyRuntimeError::new_err(
                "a raio awaitable can be awaited only once, and only by a raio task",
            )),
        }
    }
}

/// A Python coroutine as a future: each poll resumes it with the outcome of what it awaits.
pub(crate) struct Coroutine {
    coroutine: Py<PyAny>,
    awaiting: Option<(Py<Awaitable>, PyFuture)>,
}

impl Coroutine {
    pub(crate) fn new(coroutine: Py<PyAny>) -> Self {
        Self {
            coroutine,
            awaiting: None,
        }
    }

    /// Resume the coroutine and keep the future of what it awaits next; return its end, if any.
    fn resume(&mut self, py: Python<'_>) -> Option<Outcome> {
        let yielded = match self.coroutine.bind(py).call_method1("send", (py.None(),)) {
            Ok(yielded) => yielded,
            Err(error) if error.is_instance_of::<PyStopIteration>(py) => {
                let value = error.value(py).getattr("value");
                return Some(value.map(Bound::unbind));
            }
            Err(error) => return Some(Err(error)),
        };
        let Ok(awaitable) = yielded.cast::<Awaitable>() else {
            let message = format!("a raio task awaited {yielded}, which is not a raio awaitable");
            return Some(Err(PyTypeError::new_err(message)));
        };
        let Some(future) = awaitable.borrow_mut().future.take() else {
            let message = "a raio awaitable can be awaited only once";
            return Some(Err(PyRuntimeError::new_err(message)));
        };
        self.awaiting = Some((awaitable.clone().unbind(), future));
        None
    }
}

impl Future for Coroutine {
    type Output = Outcome;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Outcome> {
        let this = &mut *self;
        Python::attach(|py| {
            loop {
                if let Some((awaitable, future)) = &mut this.awaiting {
                    let Poll::Ready(outcome) = future.as_mut().poll(cx) else {
                        return Poll::Pending;
                    };
                    // The coroutine reads the outcome from the awaitable when it resumes.
                    awaitable.bind(py).borrow_mut().outcome = Some(outcome);
                    this.awaiting = None;
                }
                if let Some(end) = this.resume(py) {
                    return Poll::Ready(end);
                }
            }
        })
    }
}

/// Add this module's classes to the Python module.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Awaitable>()
}
