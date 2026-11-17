//! Drawing an app, which the Python half describes and raio-rs lays out and draws.

use std::path::PathBuf;

use pyo3::prelude::*;
use raio::diagram::{Diagram, DiagramNode};

use crate::coordinator::{PyConnection, connection};
use crate::ports::port_kind;

/// A node as Python gives it: its name, the name and the kind of each of its ports in the
/// order of their positions, and the names of its tasks.
type PyNode = (String, Vec<(String, String)>, Vec<String>);

/// Draw the nodes and connections of an app to an SVG file.
#[pyfunction]
fn diagram(path: PathBuf, nodes: Vec<PyNode>, connections: Vec<PyConnection>) -> PyResult<()> {
    let nodes = nodes.into_iter().map(|(name, ports, tasks)| {
        let ports = ports.into_iter();
        DiagramNode {
            name,
            ports: ports.map(|(name, kind)| (name, port_kind(&kind))).collect(),
            tasks,
        }
    });
    let diagram = Diagram {
        nodes: nodes.collect(),
        connections: connections.into_iter().map(connection).collect(),
    };
    Ok(diagram.write(path)?)
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(diagram, module)?)
}
