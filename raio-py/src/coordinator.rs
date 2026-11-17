use std::collections::HashMap;
use std::thread::JoinHandle;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use raio::app::{Connection, Graph, PortId};
use raio::time::Time;

/// The coordinator of an app, running on its own thread.
#[pyclass(module = "raio")]
struct Coordinator {
    endpoint: String,
    thread: Option<JoinHandle<Result<(), String>>>,
}

#[pymethods]
impl Coordinator {
    /// The address that the app's nodes register at.
    #[getter]
    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// True once the coordinator has refused the graph or every node has disconnected.
    fn finished(&self) -> bool {
        self.thread
            .as_ref()
            .is_none_or(|thread| thread.is_finished())
    }

    /// Wait until every node has disconnected; raise if the coordinator refused the graph.
    fn join(&mut self, py: Python<'_>) -> PyResult<()> {
        let Some(thread) = self.thread.take() else {
            return Ok(());
        };
        match py.detach(move || thread.join()) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(refusal)) => Err(PyValueError::new_err(refusal)),
            Err(_) => Err(PyRuntimeError::new_err("the coordinator failed")),
        }
    }
}

/// One connection: source node and port, target node and port, delay in nanoseconds.
type Edge = (usize, usize, usize, usize, i64);

/// Start the coordinator of an app with this many nodes and these connections.
///
/// `code` maps a node's id to what the node gets when it asks the coordinator for its code.
#[pyfunction]
fn coordinator(nodes: usize, connections: Vec<Edge>, code: HashMap<usize, Vec<u8>>) -> Coordinator {
    let port = |node_id, port_id| PortId { node_id, port_id };
    let connections = connections
        .into_iter()
        .map(
            |(from_node, from_port, to_node, to_port, delay)| Connection {
                from: port(from_node, from_port),
                to: port(to_node, to_port),
                delay: Time::new(delay),
            },
        )
        .collect();
    let (endpoint, thread) = raio::coordinator::spawn(Graph { nodes, connections }, code);
    Coordinator {
        endpoint,
        thread: Some(thread),
    }
}

/// Add this module's class and function to the Python module.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Coordinator>()?;
    module.add_function(wrap_pyfunction!(coordinator, module)?)?;
    Ok(())
}
