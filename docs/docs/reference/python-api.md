---
sidebar_position: 1
---

# Python API

The package is installed as `raio-py` and imported as `raio`. Everything below is available from the top-level module:

```python
import raio
from raio import Action, InputPort, Node, OutputPort, reaction, task
```

## Nodes

A node is a class that inherits from `Node`. Its ports are class attributes, its settings are the arguments of `__init__`, and its work is in methods marked as tasks or reactions.

| | |
|---|---|
| `Node` | The base class of every node. |
| `@task` | Runs an `async` method when the node starts. |
| `@reaction(*triggers)` | Runs a method once per timestamp at which a trigger has a value. A trigger is an input port or an action of the node. |
| `def shutdown(self)` | Runs once when the node shuts down. |
| `RustNode` | The base class of a node written in Rust. The subclass declares the ports and a `start(node_id, coordinator_endpoint)` method that returns the program's `subprocess.Popen`. |

## Ports

A port is declared with the type of its values in brackets, as in `OutputPort[str]()`. A port can be used only by its own node, while that node runs.

| | |
|---|---|
| `OutputPort[T]()` | Sends at most one value per timestamp to every connected input. |
| `out.set(value)` | Sets the value sent when the current timestamp completes. |
| `out.reset()` | Clears the value set at the current timestamp. |
| `InputPort[T]()` | Receives the values of one connected output. |
| `Action[T]()` | Receives values sent from any thread, as physical events. |
| `action.send(value)` | Delivers a value at a new physical timestamp. |
| `action.close()` | Ends the action after the values already sent. |

Input ports and actions are read in the same way:

| | |
|---|---|
| `port.value` | The latest value, or `None` before the first one. |
| `port.time` | The timestamp of the latest value, or `Time.NEVER` before the first one. |
| `port.present` | `True` if a value arrived at the current timestamp. |
| `port.closed` | `True` if no more values will arrive. |
| `await port` | The next value after the current timestamp. Raises `raio.Closed` if the port closes. |
| `async for value in port` | Every value until the port closes. |

## Apps

An app is built by connecting the ports of node instances and passing the nodes to `raio.run`.

| | |
|---|---|
| `raio.connect(out, inp)` | Sends every value of an output port to an input port. |
| `raio.connect(out, inp, delay=d)` | The same, with each value `d` later in logical time. |
| `raio.connect(out, inp, physical=True)` | Each value gets its timestamp from the receiver's clock on arrival. Takes no delay. |
| `raio.connect(out, inp, serializer=s)` | Uses `s` in place of pickle for this connection. |
| `raio.run(*nodes)` | Runs each node in its own process until all finish. |
| `raio.run(*nodes, fast_forward=True)` | The same, without waiting for the wall clock. |
| `raio.diagram(path, *nodes)` | Draws the nodes and their connections to an SVG file. |

## Time and Waiting

These functions are called from a task or a reaction of a running node.

| | |
|---|---|
| `await raio.wait(duration)` | Waits for a logical duration and returns the new timestamp. |
| `raio.now()` | The current logical timestamp of the node. |
| `raio.physical_now()` | The wall-clock time since the node started. |
| `raio.is_physical()` | `True` if the current timestamp was caused by an event from outside the node. |
| `await raio.select(a, b)`, `await raio.select(x=a, y=b)` | Waits for the first to finish and drops the others. With names, returns a dict of the one that finished. |
| `await raio.gather(a, b)` | Awaits everything concurrently and returns the results in order. |
| `await raio.timeout(duration, awaitable)` | Awaits something, or raises `TimeoutError` once the logical duration has passed. |
| `raio.spawn(coroutine)` | Starts a coroutine as another task of the node. Returns a `Task`; `task.done()` tells whether it has finished. |
| `raio.request_shutdown()` | Asks every node to shut down a microstep after the latest of their current timestamps. |
| `raio.request_shutdown_after(delay)` | The same, `delay` later. |

:::note

Tasks do not run on an asyncio event loop. `asyncio.sleep` and libraries built on asyncio do not work inside a task.

:::

## Time Values

A `Time` is a timestamp or a duration. It is made by multiplying a number by a unit, as in `20 * MS`.

| | |
|---|---|
| `NS`, `US`, `MS`, `S`, `SECOND`, `MINUTE`, `HOUR` | The units. |
| `Time(nanos, microstep)` | A time from its two parts. |
| `time.nanos`, `time.microstep` | The two parts. |
| `Time.ZERO`, `Time.NEVER`, `Time.FOREVER` | Zero, before every timestamp, and after every timestamp. |

## Serializers

A **serializer** converts the values of a connection to and from bytes. Connections between Python nodes use pickle by default.

| | |
|---|---|
| `MessagePack()` | Serializes what Python nodes and Rust nodes both read. Used automatically between a Python node and a Rust node. |
| Your own | Any object with `serialize(value) -> bytes` and `deserialize(data) -> value`. |

## Version

`raio.__version__` is the version of the package. A build from a git checkout appends its commit hash, as in `0.2.0+abc1234`.
