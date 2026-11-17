//! The coordinator of an app, which the launching Python process starts on a thread.

use std::collections::HashMap;
use std::thread::JoinHandle;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use raio::coordinator::{Connection, CoordinatorHandle, Failure, Graph, PortId};

use crate::time::PyTime;

/// The coordinator of an app, running on its own thread.
#[pyclass(module = "raio")]
struct Coordinator {
    endpoint: String,
    thread: Option<JoinHandle<Result<(), Failure>>>,
    handle: CoordinatorHandle,
}

#[pymethods]
impl Coordinator {
    /// The address that the app's nodes register at.
    #[getter]
    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// True once the coordinator has ended the app.
    fn finished(&self) -> bool {
        self.thread
            .as_ref()
            .is_none_or(|thread| thread.is_finished())
    }

    /// Have every node of the app shut down at once; the launcher calls this on ^C.
    fn shutdown_now(&self) {
        self.handle.shutdown_now();
    }

    /// Report that the process of a node has exited. A node that failed ends the app.
    fn node_exited(&self, node_id: usize, success: bool) {
        self.handle.node_exited(node_id, success);
    }

    /// Wait until the coordinator has ended the app; raise if it refused the graph.
    ///
    /// Return the nodes that failed, the first of them first, and those of them that went
    /// silent, whose processes may still run.
    fn join(&mut self, py: Python<'_>) -> PyResult<(Vec<usize>, Vec<usize>)> {
        let Some(thread) = self.thread.take() else {
            return Ok((Vec::new(), Vec::new()));
        };
        match py.detach(move || thread.join()) {
            Ok(Ok(())) => Ok((Vec::new(), Vec::new())),
            Ok(Err(Failure::Nodes { failed, silent })) => Ok((failed, silent)),
            Ok(Err(Failure::Refused(refusal))) => Err(PyValueError::new_err(refusal)),
            Err(_) => Err(PyRuntimeError::new_err("the coordinator failed")),
        }
    }
}

/// A connection as Python gives it: source node and port, target node and port, the delay, and
/// whether it is physical.
pub(crate) type PyConnection = (usize, usize, usize, usize, Py<PyTime>, bool);

pub(crate) fn connection(connection: PyConnection) -> Connection {
    let (from_node, from_port, to_node, to_port, delay, physical) = connection;
    let port = |node_id, port_id| PortId { node_id, port_id };
    Connection {
        from: port(from_node, from_port),
        to: port(to_node, to_port),
        delay: delay.get().0,
        physical,
    }
}

/// Start the coordinator of an app with this many nodes and these connections.
///
/// `code` maps a node's id to what the node gets when it asks the coordinator for its code.
#[pyfunction]
fn coordinator(
    nodes: usize,
    connections: Vec<PyConnection>,
    code: HashMap<usize, Vec<u8>>,
) -> Coordinator {
    let connections = connections.into_iter().map(connection).collect();
    let graph = Graph { nodes, connections };
    let (endpoint, thread, handle) = raio::coordinator::spawn(graph, code);
    Coordinator {
        endpoint,
        thread: Some(thread),
        handle,
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Coordinator>()?;
    module.add_function(wrap_pyfunction!(coordinator, module)?)?;
    Ok(())
}
