---
sidebar_position: 1
---

# Architecture

See [CONTRIBUTING.md](https://github.com/raio-robotics/raio/blob/main/CONTRIBUTING.md) for how to build and test.

## Processes

An app is one process per node, plus the **launcher**: the process that called `run`. The **coordinator** runs inside the launcher.

A node talks to the coordinator to start up, to report that it is alive, and to shut down. Port values do not pass through the coordinator: nodes send them straight to each other over TCP.

:::note

Each node listens on `127.0.0.1`, so the nodes of an app currently run on one machine.

:::

## Crates

The repository is one Cargo workspace with three crates. `raio-rs`, in `core`, is the runtime and the Rust API. `raio-macros`, in `core/macros`, holds the `Ports` derive macro, which Rust requires to be a crate of its own. `raio-py`, in `python`, is the Python binding.

## Modules of the Core

The core is divided into these modules:

| Module | What it does |
|---|---|
| `app` | What a node program is written with: `App`, ports, actions, reactions, connections. |
| `node` | The scheduler that orders the events of one node by timestamp. |
| `coordinator` | The graph of an app; wires its nodes at startup and shuts them down. |
| `transport` | Carries bytes between processes. |
| `serializer` | Converts port values to and from bytes: bincode and MessagePack. |
| `messages` | Everything that crosses a process boundary. |
| `runtime` | The single-threaded executor under each node. |
| `driver` | The executor's I/O and timers. |
| `diagram` | Draws an app. |

## One Thread per Node

A node runs all of its tasks on a single-threaded executor of its own, so the tasks of a node never run in parallel and need no locks.

The executor and the scheduler work together. Each waker says whether its task resumes at the node's current timestamp or at a new physical timestamp, and the scheduler applies that when it decides which timestamp the node handles next.
