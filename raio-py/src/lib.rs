mod awaitable;
mod coordinator;
mod node;
mod ports;
mod time;

use pyo3::prelude::*;

/// The part of raio that runs on raio-rs: a node's runtime, its ports and its clock.
#[pymodule]
fn core(module: &Bound<'_, PyModule>) -> PyResult<()> {
    time::register(module)?;
    awaitable::register(module)?;
    ports::register(module)?;
    node::register(module)?;
    coordinator::register(module)
}
