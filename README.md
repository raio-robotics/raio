<!-- markdownlint-disable MD001 MD041 -->
<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/raio-robotics/raio/main/docs/assets/logo/raio-lockup-white-rounded.png">
    <img alt="RAIO" src="https://raw.githubusercontent.com/raio-robotics/raio/main/docs/assets/logo/raio-lockup-black-rounded.png" width=40%>
  </picture>
</p>

<h3 align="center">
RAIO: Realtime Asynchronous Deterministic Robotic Middleware
</h3>

<p align="center">
<a href="https://raio-robotics.com"><b>Docs</b></a> | <a href="https://raio-robotics.com/blog"><b>Blog</b></a> | <a href="https://join.slack.com/t/raio-hq/shared_invite/zt-4h90ce5q9-adb~cElnfDwekfEjcY0BtA"><b>Slack</b></a> |
<a href="https://pypi.org/project/raio-py/"><img alt="PyPI version" src="https://img.shields.io/pypi/v/raio-py" align="absmiddle"></a>
<a href="https://crates.io/crates/raio-rs"><img alt="crates.io version" src="https://img.shields.io/crates/v/raio-rs" align="absmiddle"></a>
<a href="https://github.com/raio-robotics/raio/blob/main/LICENSE"><img alt="License: Apache-2.0" src="https://img.shields.io/badge/license-Apache--2.0-blue" align="absmiddle"></a>
</p>

---

Raio is a realtime robotic middleware that is deterministic. Time is treated as a first order primitive, integrated with the `async` / `await` syntax.

## Get Started

Install Raio for Python 3.9 or later:

```bash
pip install raio-py
```

Raio is built with Rust, to add it as a dependency:
```bash
cargo add raio-rs
```

## Hello World

Two nodes, each in its own process: `Sender` sends a value and `Listener` prints it.

```python
import raio
from raio import MS, InputPort, Node, OutputPort, reaction, task

class Sender(Node):
    out = OutputPort[str]()

    @task
    async def send(self):
        await raio.wait(1 * MS)
        self.out.set("hello world")

class Listener(Node):
    inp = InputPort[str]()

    @reaction(inp)
    def show(self):
        print(self.inp.value)

sender, listener = Sender(), Listener()
raio.connect(sender.out, listener.inp)
raio.run(sender, listener)
```

```console
$ python hello.py
hello world
```

## Concepts

| | |
|---|---|
| **Node** | A unit of computation with its own process, ports, tasks, and logical clock. |
| **Timestamp** | A point in logical time: nanoseconds since the app started, and a microstep that orders events within one nanosecond. |
| **Task** | An async function that starts with its node. It waits for values and for logical time to pass, and sets outputs. |
| **Reaction** | A function that runs once at each timestamp at which one of its input ports or actions has a value. |
| **Output port** | Holds at most one value per timestamp and sends it when the timestamp completes. It may feed any number of inputs. |
| **Input port** | Receives the values of exactly one output, each at the sender's timestamp plus the delay of the connection. |
| **Action** | An input that threads of the node's own program feed, such as a sensor driver. It takes no connection. |
| **Coordinator** | Runs in the process that launches the app. It wires the nodes at startup, hears their heartbeats, and is the one that shuts the app down. |

How a node runs:

- At each timestamp, the tasks and reactions that have something to do run one after another,
  in the order they are declared. When none of them can go on, the timestamp is complete and
  the node sends what its outputs hold.
- With every timestamp, a node also promises its receivers the earliest timestamp at which it
  may send again. A receiver handles a timestamp only once the promise of each of its senders
  is beyond it.
- Before a sender's first promise, a receiver has the one that the coordinator works out from
  the graph: nothing arrives until the delay of the connection has passed. This is what lets a
  loop of connections start. The loop needs a delay, and one of its nodes has to act before
  that delay has passed. A loop with a physical connection needs neither.
- A node handles a timestamp no earlier than the wall clock reaches it. In fast-forward mode it
  does not wait for the wall clock, which suits tests and replays.

How an app ends:

- An app is over when every node has finished its tasks.
- `request_shutdown()` in a node shuts down every node at one timestamp, and ^C shuts them down
  at once. Either way each node runs its shutdown function.
- A running node sends the coordinator a heartbeat twice a second. When a node fails, or the
  coordinator has not heard from it for 5 seconds, the coordinator shuts down the other nodes
  and the launcher reports the one that failed. So nothing in a node may block its thread for
  that long: not a task, a reaction, or its shutdown function.
- A node that loses its coordinator shuts down at once.

## Consistency and availability

A connection keeps an app **consistent**: a receiver never runs ahead of what its senders have
promised, so the order of events is the same on every run. The price is **availability**: a
node that waits for a slow sender does not advance. These are the places where you choose.

| You use | You get | You give up |
|---|---|---|
| A connection | The same events at the same timestamps on every run. | A receiver waits for its senders. |
| A connection with a delay | A receiver that runs ahead of its sender by up to the delay, and is still deterministic. | Each value arrives that much later in logical time. |
| A physical connection | A receiver that never waits for its sender. | Each value gets its timestamp from the receiver's wall clock when it arrives, which differs between runs. `is_physical()` is true at such a timestamp. |
| An action, or in Rust a future that raio does not provide | The event is handled when it arrives, and nothing waits for it. | Its timestamp comes from the wall clock and differs between runs. `is_physical()` is true at such a timestamp. |
| `request_shutdown()` | Every node shuts down at one timestamp. | The nodes run on until that timestamp. |
| ^C | Every node runs its shutdown function at once. | The nodes stop at different timestamps. |

## Rust and Python side by side

| | Rust | Python |
|---|---|---|
| Wait in logical time | `node.wait(10 * MS).await` | `await raio.wait(10 * MS)` |
| Next value of an input | `input.wait().await` | `await port`, `async for value in port` |
| Value at the current timestamp | `input.get()` | `port.value` when `port.present` |
| Set an output | `out.set(value)` | `out.set(value)` |
| React to inputs | `reaction!(self, [a, b], { ... })` | `@reaction(a, b)` |
| Wait for several things | `select` and `join!` of the `futures` crate | `raio.select`, `raio.gather`, `raio.timeout` |
| Start another task | `node.spawn(future)` | `raio.spawn(coroutine)` |
| Current timestamp | `node.now()` | `raio.now()` |
| Events from threads | `Action<T>` and its `handle()` | `Action[T]` and its `send` |
| Connect | `app.connect(&a.out, &b.input).delay(d)` | `raio.connect(a.out, b.inp, delay=d)` |
| Connect physically | `app.connect(&a.out, &b.input).physical()` | `raio.connect(a.out, b.inp, physical=True)` |
| Shut the app down | `node.request_shutdown()` | `raio.request_shutdown()` |
| Run at shutdown | `.on_shutdown(Ticker::shutdown)` | `def shutdown(self)` |
| Draw the app | `app.diagram("app.svg")` | `raio.diagram("app.svg", a, b)` |

## Diagram

`diagram` draws an app to an SVG file in the style of Lingua Franca: each node with its ports
and its tasks, and each connection with its delay. A physical connection is a dashed line. [elkrs](https://github.com/depetrol/elkrs)
places the nodes and routes the connections. To see one, run
`cargo run --example diagram -- app.svg`.

A node is shown by the name of its type, with its id in the corner, and a task by the name of
its method. A port's name is inside its node, next to the port. A Python port has the name of
its attribute. A Rust port has the name of the field that holds it, which `#[derive(Ports)]`
reads; every Rust node derives it. A port that no field holds is shown by its position.

```rust
#[derive(Ports)]
struct Sensor {
    node: Node,
    reading: OutputPort<i64>,
}
```

## Rust and Python in one app

The graph of a mixed app is written in Python. A Rust node is a program of its own that calls
`run_node`; in Python it appears as a `RustNode` subclass that declares the node's ports, in
the order the Rust node builds them, and starts the program:

```python
class Scale(RustNode):
    inp = InputPort[dict]()
    out = OutputPort[dict]()

    def __init__(self, factor: float):
        self.factor = factor

    def start(self, node_id, coordinator_endpoint):
        command = [str(PROGRAM), str(self.factor)]
        command += ["--node_id", str(node_id), "--coordinator_endpoint", coordinator_endpoint]
        return subprocess.Popen(command)
```

A connection between a Python node and a Rust node uses MessagePack; the Rust node puts
`MessagePackSerializer` on the ports concerned. See `core/examples/scale.rs` and
`python/examples/mixed.py`.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to build RAIO from source, run the tests, and make
a release.
