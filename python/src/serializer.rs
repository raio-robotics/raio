//! The values that Python ports carry, and what serializes them.

use std::rc::Rc;

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyBytes;
use raio::serializer::{Payload, Serializer};

static DUMPS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static LOADS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

/// A Python value carried by a port.
pub(crate) struct PyValue(pub(crate) Py<PyAny>);

/// A connection between Python nodes uses pickle unless it is given a serializer.
impl Payload for PyValue {
    fn default_serializer() -> Rc<dyn Serializer<Self>> {
        Rc::new(PickleSerializer)
    }
}

struct PickleSerializer;

impl Serializer<PyValue> for PickleSerializer {
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

/// A serializer written in Python: an object with `serialize` and `deserialize` methods.
pub(crate) struct PySerializer {
    pub(crate) name: String,
    pub(crate) serializer: Py<PyAny>,
}

impl Serializer<PyValue> for PySerializer {
    fn name(&self) -> &str {
        &self.name
    }

    fn serialize(&self, value: &PyValue) -> Vec<u8> {
        Python::attach(|py| {
            let serializer = self.serializer.bind(py);
            let data = serializer
                .call_method1("serialize", (value.0.bind(py),))
                .unwrap_or_else(|error| panic!("{} cannot serialize a value: {error}", self.name));
            let Ok(data) = data.cast::<PyBytes>() else {
                panic!("{} must serialize a value to bytes, not {data}", self.name);
            };
            data.as_bytes().to_vec()
        })
    }

    fn deserialize(&self, bytes: &[u8]) -> PyValue {
        Python::attach(|py| {
            let serializer = self.serializer.bind(py);
            let value = serializer
                .call_method1("deserialize", (PyBytes::new(py, bytes),))
                .unwrap_or_else(|error| {
                    panic!("{} cannot deserialize a value: {error}", self.name)
                });
            PyValue(value.unbind())
        })
    }
}
