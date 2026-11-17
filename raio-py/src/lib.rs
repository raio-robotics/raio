//! The Rust half of raio's Python binding. Python describes a node and its tasks; this crate
//! runs them on raio-rs: each task's coroutine is driven as a raio task, with no asyncio.

mod awaitable;
mod combine;
mod coordinator;
mod node;
mod ports;
mod run;
mod serializer;
mod task;
mod time;

use pyo3::prelude::*;

/// The part of raio that runs on raio-rs: a node's runtime, its ports and its clock.
#[pymodule]
fn core(module: &Bound<'_, PyModule>) -> PyResult<()> {
    time::register(module)?;
    awaitable::register(module)?;
    combine::register(module)?;
    ports::register(module)?;
    node::register(module)?;
    task::register(module)?;
    run::register(module)?;
    coordinator::register(module)
}
