//! `Time` as Python has it.

use pyo3::exceptions::{PyOverflowError, PyZeroDivisionError};
use pyo3::prelude::*;
use pyo3::types::PyType;
use raio::time::{HOUR, MINUTE, MS, NS, S, SECOND, Time, US};

/// A logical timestamp or duration in nanoseconds and microsteps.
#[pyclass(
    name = "Time",
    module = "raio",
    frozen,
    eq,
    ord,
    hash,
    skip_from_py_object
)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PyTime(pub(crate) Time);

/// A number that a time can be multiplied by.
#[derive(FromPyObject)]
enum Factor {
    Int(i64),
    Float(f64),
}

/// A time or a number that a time can be divided by.
#[derive(FromPyObject)]
enum Divisor {
    Time(Py<PyTime>),
    Int(i64),
    Float(f64),
}

fn out_of_range() -> PyErr {
    PyOverflowError::new_err("the time is out of range")
}

/// Round a nanosecond count to the nearest integer, with ties to even as Python's `round` does.
fn rounded(nanos: f64) -> PyResult<PyTime> {
    let nanos = nanos.round_ties_even();
    if nanos.is_finite() && nanos > i64::MIN as f64 && nanos < i64::MAX as f64 {
        Ok(PyTime(Time::new(nanos as i64)))
    } else {
        Err(out_of_range())
    }
}

impl PyTime {
    fn is_sentinel(&self) -> bool {
        self.0 == Time::NEVER || self.0 == Time::FOREVER
    }

    fn scaled(&self, factor: Factor) -> PyResult<PyTime> {
        if self.is_sentinel() {
            return Ok(*self); // NEVER and FOREVER absorb every factor.
        }
        match factor {
            Factor::Int(factor) => {
                let nanos = self.0.nanos().checked_mul(factor);
                nanos
                    .map(|nanos| PyTime(Time::new(nanos)))
                    .ok_or_else(out_of_range)
            }
            Factor::Float(factor) => rounded(self.0.nanos() as f64 * factor),
        }
    }
}

#[pymethods]
impl PyTime {
    #[new]
    #[pyo3(signature = (nanos = 0, microstep = 0))]
    fn new(nanos: i64, microstep: u32) -> Self {
        let mut time = Time::new(nanos);
        for _ in 0..microstep {
            time = time.next_microstep();
        }
        PyTime(time)
    }

    #[classattr]
    const ZERO: PyTime = PyTime(Time::ZERO);

    #[classattr]
    const NEVER: PyTime = PyTime(Time::NEVER);

    #[classattr]
    const FOREVER: PyTime = PyTime(Time::FOREVER);

    /// The nanoseconds of this time.
    #[getter]
    fn nanos(&self) -> i64 {
        self.0.nanos()
    }

    /// The position among the timestamps that share these nanoseconds.
    #[getter]
    fn microstep(&self) -> u32 {
        self.0.microstep()
    }

    fn __add__(&self, other: &PyTime) -> PyTime {
        PyTime(self.0 + other.0)
    }

    fn __sub__(&self, other: &PyTime) -> PyTime {
        PyTime(self.0 - other.0)
    }

    fn __mul__(&self, factor: Factor) -> PyResult<PyTime> {
        self.scaled(factor)
    }

    fn __rmul__(&self, factor: Factor) -> PyResult<PyTime> {
        self.scaled(factor)
    }

    /// Divide by a time to get a ratio, or by a number to get a shorter time.
    fn __truediv__(&self, py: Python<'_>, divisor: Divisor) -> PyResult<Py<PyAny>> {
        let nanos = self.0.nanos() as f64;
        let (divided_by, ratio) = match divisor {
            Divisor::Time(time) => (time.get().0.nanos() as f64, true),
            Divisor::Int(number) => (number as f64, false),
            Divisor::Float(number) => (number, false),
        };
        if divided_by == 0.0 {
            return Err(PyZeroDivisionError::new_err("division of a time by zero"));
        }
        if ratio {
            return Ok((nanos / divided_by).into_pyobject(py)?.into_any().unbind());
        }
        let time = match self.is_sentinel() {
            true => *self,
            false => rounded(nanos / divided_by)?,
        };
        Ok(Py::new(py, time)?.into_any())
    }

    /// Count how many times another time fits into this one.
    fn __floordiv__(&self, other: &PyTime) -> PyResult<i64> {
        if other.0.nanos() == 0 {
            return Err(PyZeroDivisionError::new_err("division of a time by zero"));
        }
        Ok(self.0.nanos().div_euclid(other.0.nanos()))
    }

    fn __str__(&self) -> String {
        self.0.to_string()
    }

    fn __repr__(&self) -> String {
        format!("Time({})", self.0)
    }

    /// Pickle a time as its nanoseconds and microstep.
    fn __reduce__<'py>(slf: &Bound<'py, Self>) -> (Bound<'py, PyType>, (i64, u32)) {
        let time = slf.get().0;
        (slf.get_type(), (time.nanos(), time.microstep()))
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyTime>()?;
    let units = [
        ("NS", NS),
        ("US", US),
        ("MS", MS),
        ("S", S),
        ("SECOND", SECOND),
        ("MINUTE", MINUTE),
        ("HOUR", HOUR),
    ];
    for (name, unit) in units {
        module.add(name, PyTime(unit))?;
    }
    Ok(())
}
