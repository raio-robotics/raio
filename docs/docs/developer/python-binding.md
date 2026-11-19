---
sidebar_position: 3
---

# The Python Binding

## Two Halves

The binding has two halves. The Python half, in `python/python/raio`, provides `Node`, the ports, `connect`, and `run`: it describes a node and its tasks. The Rust half, in `python/src`, runs them on the core: each task's coroutine is driven as a Raio task.

## How a Node Reaches Its Process

A node is built in the launcher but runs in a process of its own. `raio.run` pickles each node with cloudpickle, the coordinator hands the node's process its code, and that process rebuilds the node. The core then builds the node's ports and runs its tasks.

Two things follow from this. First, a node must be picklable when `run` is called. A lock, a socket, or a thread created in `__init__` fails with `cannot pickle`. Such things are created inside a task, which runs in the node's own process.

Second, a port works only in its own node, while that node runs. Reading `listener.inp.value` from the launcher raises an error.

## No asyncio

A task's coroutine is driven by the core's executor, not by an asyncio event loop. A task awaits Raio's own awaitables: `raio.wait`, ports, `select`, `gather`, and `timeout`.

:::note

`asyncio.sleep` in a task raises `RuntimeError: no running event loop`.

:::

## Building

The binding is built with [maturin](https://www.maturin.rs):

```bash
cd python
maturin develop
python -m pytest tests/
```
