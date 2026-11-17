//! Launching an app: one process per node. The launcher reports what it sees to the
//! coordinator, a ^C or the exit of a node's process, and the coordinator shuts the app down.

use std::collections::HashMap;
use std::process::{Child, Command};
use std::thread::JoinHandle;
use std::time::Duration;

use super::interrupt;
use super::run::Assignment;
use crate::coordinator::{self, CoordinatorHandle, Failure, Graph};

/// Start this program once for each node, wire the nodes through a coordinator, and wait
/// until the app has ended. A node that fails ends the app.
pub(super) fn launch(graph: Graph) {
    let exe = std::env::current_exe().unwrap();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let nodes = graph.nodes;
    // Every node is this program, so the coordinator holds no code for them.
    let (endpoint, coordinator, handle) = coordinator::spawn(graph, HashMap::new());
    // On ^C every node runs its shutdown function at once, and the app ends as it otherwise does.
    let interrupted = handle.clone();
    interrupt::on_interrupt(move || interrupted.shutdown_now());
    let mut children: Vec<_> = (0..nodes)
        .map(|node_id| {
            let assignment = Assignment {
                node_id,
                coordinator_endpoint: endpoint.clone(),
            };
            // The program's own arguments keep their positions.
            Command::new(&exe)
                .args(&args)
                .args(assignment.args())
                .spawn()
                .unwrap()
        })
        .collect();
    report_exits(&mut children, &coordinator, &handle);
    let outcome = coordinator
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    // The coordinator has given up on the nodes that went silent, so nothing waits for them.
    if let Err(Failure::Nodes { silent, .. }) = &outcome {
        for node_id in silent {
            children[*node_id].kill().unwrap();
        }
    }
    let statuses: Vec<_> = children
        .iter_mut()
        .map(|child| child.wait().unwrap())
        .collect();
    match outcome {
        Err(Failure::Refused(refusal)) => panic!("{refusal}"),
        Err(Failure::Nodes { failed, silent }) if silent.contains(&failed[0]) => {
            panic!("node {} stopped responding", failed[0])
        }
        Err(Failure::Nodes { failed, .. }) => {
            panic!("node {} exited with {}", failed[0], statuses[failed[0]])
        }
        Ok(()) => {
            // A node may still fail once the coordinator has seen it end.
            if let Some(node_id) = statuses.iter().position(|status| !status.success()) {
                panic!("node {node_id} exited with {}", statuses[node_id]);
            }
        }
    }
}

/// Report the exit of each node's process to the coordinator, until it has ended the app.
fn report_exits<T>(
    children: &mut [Child],
    coordinator: &JoinHandle<T>,
    handle: &CoordinatorHandle,
) {
    let mut reported = vec![false; children.len()];
    while !coordinator.is_finished() {
        for (node_id, child) in children.iter_mut().enumerate() {
            if !reported[node_id]
                && let Some(status) = child.try_wait().unwrap()
            {
                handle.node_exited(node_id, status.success());
                reported[node_id] = true;
            }
        }
        // The launcher only looks on, so it need not see an exit the moment it happens.
        std::thread::sleep(Duration::from_millis(10));
    }
}
