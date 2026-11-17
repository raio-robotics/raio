//! Running a node in its own process: the node's code comes from the coordinator, Python
//! rebuilds the node from it, and raio-rs builds the ports and runs the tasks.

use std::cell::RefCell;
use std::rc::Rc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use raio::app::{Builder, ExecutionMode, Start, create_node, run_node_on};
use raio::coordinator::Link;
use raio::runtime::Runtime;

use crate::awaitable::to_future;
use crate::node::{RUNNING, RunningNode};
use crate::ports::{ACTION_SENDERS, Port};
use crate::serializer::PySerializer;
use crate::task::spawn_task;

/// A node's code as Python loaded it: all that raio-rs needs to build the node and run it.
#[derive(FromPyObject)]
struct Loaded {
    /// The kind of each port, in the order of their positions: input, output or action.
    kinds: Vec<String>,
    /// The serializers that connections chose: the port's position, the name, the serializer.
    serializers: Vec<(usize, String, Py<PyAny>)>,
    /// Returns the coroutines of the node's tasks, in the order the tasks are declared.
    start: Py<PyAny>,
    shutdown: Py<PyAny>,
}

/// Build the ports in the order Python numbered them, and give them their serializers.
fn build_ports(
    builder: &mut Builder,
    kinds: &[String],
    serializers: Vec<(usize, String, Py<PyAny>)>,
) -> Vec<Port> {
    let ports = kinds
        .iter()
        .enumerate()
        .map(|(index, kind)| match kind.as_str() {
            "input" => Port::Input(builder.input()),
            "output" => Port::Output(builder.output()),
            "action" => {
                let action = builder.action();
                ACTION_SENDERS
                    .lock()
                    .unwrap()
                    .insert(index, action.handle());
                Port::Action(action)
            }
            kind => panic!("unknown port kind {kind:?}"),
        });
    let ports: Vec<Port> = ports.collect();
    for (index, name, serializer) in serializers {
        let serializer = PySerializer { name, serializer };
        match ports.get(index) {
            // An input has the one serializer of its connection.
            Some(Port::Input(input)) => input.set_serializer(serializer),
            // An output keeps one for each choice that its connections make.
            Some(Port::Output(output)) => output.add_serializer(serializer),
            _ => panic!("port {index} of this node takes no serializer"),
        }
    }
    ports
}

/// Make the node the one that Python reaches on this thread, and spawn its tasks.
async fn start_tasks(running: Rc<RunningNode>, start: Py<PyAny>) {
    RUNNING.set(Some(running.clone()));
    let tasks = Python::attach(|py| {
        let coroutines = start.call0(py)?.extract::<Vec<Py<PyAny>>>(py)?;
        let tasks = coroutines.iter().map(|task| to_future(task.bind(py)));
        tasks.collect::<PyResult<Vec<_>>>()
    });
    match tasks {
        Ok(tasks) => {
            for task in tasks {
                spawn_task(&running, task);
            }
        }
        Err(error) => running.fail(error),
    }
}

/// Carries the runtime's blocking wait across the release of the GIL.
struct IdleWait<'a>(&'a mut dyn FnMut());

// SAFETY: The wait runs on the thread that created it; only the GIL is released around it.
unsafe impl Send for IdleWait<'_> {}

/// Release the GIL while the node's thread waits for its next event.
fn release_gil(wait: &mut dyn FnMut()) {
    let wait = IdleWait(wait);
    Python::attach(|py| {
        py.detach(move || {
            let wait = wait; // Captures the whole `IdleWait`; its field alone is not `Send`.
            (wait.0)()
        })
    });
}

/// Run one node in this process until it finishes.
///
/// `load` turns the code that the coordinator holds for the node into what `Loaded` lists.
#[pyfunction]
fn run_node(
    py: Python<'_>,
    node_id: usize,
    coordinator_endpoint: String,
    load: Py<PyAny>,
    fast_forward: bool,
) -> PyResult<()> {
    let mut link = Link::open(node_id, &coordinator_endpoint);
    let code = link.code.take().ok_or_else(|| {
        PyRuntimeError::new_err(format!("the coordinator holds no code for node {node_id}"))
    })?;
    let loaded = load.call1(py, (PyBytes::new(py, &code),))?;
    let Loaded {
        kinds,
        serializers,
        start,
        shutdown,
    } = loaded.extract(py)?;
    let build = move |builder: &mut Builder| {
        if fast_forward {
            builder.execution_mode(ExecutionMode::FastForward);
        }
        RunningNode {
            ports: build_ports(builder, &kinds, serializers),
            node: builder.node(),
            failure: RefCell::new(None),
        }
    };
    let start: Start<RunningNode> = Box::new(move |running| Box::pin(start_tasks(running, start)));
    Runtime::set_idle_hook(release_gil);
    let node = create_node(build, vec![start]).on_shutdown(move |running: &RunningNode| {
        // An error in the shutdown method fails the node, as an error in a task does.
        if let Err(error) = Python::attach(|py| shutdown.call0(py)) {
            running.fail(error);
        }
    });
    run_node_on(link, node);
    ACTION_SENDERS.lock().unwrap().clear(); // A value sent from now on finds its action closed.
    let failure = RUNNING.take().and_then(|running| running.failure.take());
    failure.map_or(Ok(()), Err)
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(run_node, module)?)
}
