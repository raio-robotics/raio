---
sidebar_position: 3
---

# Examples

The repository has an example program for every feature of Raio, in both languages. Most Python examples have a Rust example of the same name that does the same thing.

## Python

Run one with `python python/examples/<name>.py` from the repository.

| Example | Shows |
|---|---|
| [`hello.py`](https://github.com/raio-robotics/raio/blob/main/python/examples/hello.py) | Two nodes and one connection. |
| [`timer.py`](https://github.com/raio-robotics/raio/blob/main/python/examples/timer.py) | A timer made of `raio.wait` in a loop. |
| [`ticker.py`](https://github.com/raio-robotics/raio/blob/main/python/examples/ticker.py) | A node with settings, used by other examples. |
| [`reaction.py`](https://github.com/raio-robotics/raio/blob/main/python/examples/reaction.py) | One reaction triggered by two ports. |
| [`select_first.py`](https://github.com/raio-robotics/raio/blob/main/python/examples/select_first.py) | A task that waits for the first of two ports. |
| [`sync.py`](https://github.com/raio-robotics/raio/blob/main/python/examples/sync.py) | Pairing the values of two ports that share a timestamp. |
| [`join.py`](https://github.com/raio-robotics/raio/blob/main/python/examples/join.py) | Two tasks of one node at different rates. |
| [`same_timestamp.py`](https://github.com/raio-robotics/raio/blob/main/python/examples/same_timestamp.py) | A node that receives values and also sends one of its own. |
| [`action.py`](https://github.com/raio-robotics/raio/blob/main/python/examples/action.py) | Keyboard input from a thread, through an action. |
| [`ping_pong.py`](https://github.com/raio-robotics/raio/blob/main/python/examples/ping_pong.py) | A loop of connections with a delay. |
| [`physical.py`](https://github.com/raio-robotics/raio/blob/main/python/examples/physical.py) | A loop closed by a physical connection. |
| [`serializer.py`](https://github.com/raio-robotics/raio/blob/main/python/examples/serializer.py) | A connection with its own serializer. |
| [`shutdown.py`](https://github.com/raio-robotics/raio/blob/main/python/examples/shutdown.py) | Shutting down every node at one timestamp. |
| [`interrupt.py`](https://github.com/raio-robotics/raio/blob/main/python/examples/interrupt.py) | Shutdown methods that run on Ctrl-C. |
| [`scale.py`](https://github.com/raio-robotics/raio/blob/main/python/examples/scale.py) | The Python declaration of a Rust node. |
| [`mixed.py`](https://github.com/raio-robotics/raio/blob/main/python/examples/mixed.py) | A Rust node between two Python nodes. |

## Rust

Run one with `cargo run --example <name>`.

| Example | Shows |
|---|---|
| [`hello.rs`](https://github.com/raio-robotics/raio/blob/main/core/examples/hello.rs) | Two nodes and one connection. |
| [`timer.rs`](https://github.com/raio-robotics/raio/blob/main/core/examples/timer.rs) | A timer made of `node.wait` in a loop. |
| [`reaction.rs`](https://github.com/raio-robotics/raio/blob/main/core/examples/reaction.rs) | One reaction triggered by two ports. |
| [`reaction_order.rs`](https://github.com/raio-robotics/raio/blob/main/core/examples/reaction_order.rs) | Reactions run in the order they are listed. |
| [`select.rs`](https://github.com/raio-robotics/raio/blob/main/core/examples/select.rs) | A task that waits for the first of two ports. |
| [`sync.rs`](https://github.com/raio-robotics/raio/blob/main/core/examples/sync.rs) | Pairing the values of two ports that share a timestamp. |
| [`join.rs`](https://github.com/raio-robotics/raio/blob/main/core/examples/join.rs) | Two tasks of one node at different rates. |
| [`same_timestamp.rs`](https://github.com/raio-robotics/raio/blob/main/core/examples/same_timestamp.rs) | A node that receives values and also sends one of its own. |
| [`fanout.rs`](https://github.com/raio-robotics/raio/blob/main/core/examples/fanout.rs) | One output that feeds two inputs, one of them with a delay. |
| [`action.rs`](https://github.com/raio-robotics/raio/blob/main/core/examples/action.rs) | Input from a thread, through an action. |
| [`foreign.rs`](https://github.com/raio-robotics/raio/blob/main/core/examples/foreign.rs) | Awaiting a future of another runtime. |
| [`ping_pong.rs`](https://github.com/raio-robotics/raio/blob/main/core/examples/ping_pong.rs) | A loop of connections with a delay. |
| [`physical.rs`](https://github.com/raio-robotics/raio/blob/main/core/examples/physical.rs) | A loop closed by a physical connection. |
| [`serializer.rs`](https://github.com/raio-robotics/raio/blob/main/core/examples/serializer.rs) | A connection with its own serializer. |
| [`shutdown.rs`](https://github.com/raio-robotics/raio/blob/main/core/examples/shutdown.rs) | Shutting down every node at one timestamp. |
| [`interrupt.rs`](https://github.com/raio-robotics/raio/blob/main/core/examples/interrupt.rs) | Shutdown functions that run on Ctrl-C. |
| [`failure.rs`](https://github.com/raio-robotics/raio/blob/main/core/examples/failure.rs) | A node that fails, and the app shutting down around it. |
| [`silent.rs`](https://github.com/raio-robotics/raio/blob/main/core/examples/silent.rs) | A node that blocks its thread and misses its heartbeats. |
| [`diagram.rs`](https://github.com/raio-robotics/raio/blob/main/core/examples/diagram.rs) | Drawing an app to an SVG file. |
| [`node.rs`](https://github.com/raio-robotics/raio/blob/main/core/examples/node.rs) | A program that runs a single node of an app. |
| [`scale.rs`](https://github.com/raio-robotics/raio/blob/main/core/examples/scale.rs) | A node for a mixed Rust and Python app. |
