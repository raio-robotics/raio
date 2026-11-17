# raio

raio is a deterministic realtime async runtime for robot software. An app is a set of **nodes**,
each in its own process, that exchange values through **ports**. A node is written with
`async`/`await`, in Rust or in Python, and one app can mix the two.

Every value has a **timestamp**: a point in logical time. A node moves to a timestamp only once
no node that feeds it can still send something earlier. So every run of an app sees the same
events in the same order, however fast or slow each process happens to be, while logical time
keeps pace with the wall clock.

## A first app

A producer sends one value and a sink prints it.

Rust (`raio-rs/examples/hello.rs`, run with `cargo run --example hello`):

```rust
use raio::app::{App, Builder, InputPort, Node, OutputPort};
use raio::tasks;
use raio::time::MS;
use std::rc::Rc;

struct Producer {
    node: Node,
    out: OutputPort<String>,
}

impl Producer {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            out: builder.output(),
        }
    }

    async fn start(self: Rc<Self>) {
        self.node.wait(1 * MS).await;
        self.out.set("hello world".to_string());
    }
}

struct Sink {
    input: InputPort<String>,
}

impl Sink {
    fn new(builder: &mut Builder) -> Self {
        Self {
            input: builder.input(),
        }
    }

    async fn start(self: Rc<Self>) {
        while let Some((value, _)) = self.input.wait().await {
            println!("sink received {:?}", *value);
        }
    }
}

fn main() {
    let mut app = App::new();
    let producer = app.create_node(Producer::new, tasks![Producer::start]);
    let sink = app.create_node(Sink::new, tasks![Sink::start]);
    app.connect(&producer.out, &sink.input);
    app.run();
}
```

Python (`raio-py/examples/hello.py`, run with `python examples/hello.py`):

```python
import raio
from raio import MS, InputPort, Node, OutputPort, reaction, task


class Producer(Node):
    out = OutputPort[str]()

    @task
    async def produce(self):
        await raio.wait(1 * MS)
        self.out.set("hello world")


class Sink(Node):
    inp = InputPort[str]()

    @reaction(inp)
    def show(self):
        print(f"sink received {self.inp.value!r}")


producer, sink = Producer(), Sink()
raio.connect(producer.out, sink.inp)
raio.run(producer, sink)
```

More examples are in `raio-rs/examples` and `raio-py/examples`.

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
  that delay has passed.
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
| Shut the app down | `node.request_shutdown()` | `raio.request_shutdown()` |
| Run at shutdown | `.on_shutdown(Ticker::shutdown)` | `def shutdown(self)` |
| Draw the app | `app.diagram("app.svg")` | `raio.diagram("app.svg", a, b)` |

## Diagram

`diagram` draws an app to an SVG file in the style of Lingua Franca: each node with its ports
and its tasks, and each connection with its delay. [elkrs](https://github.com/depetrol/elkrs)
places the nodes and routes the connections. To see one, run
`cargo run --example diagram -- app.svg`.

A node is shown by the name of its type, with its id in the corner, and a task by the name of
its method. A port's name is inside its node, next to the port. A Python port has the name of
its attribute. A Rust port gets the name of its field from `ports!`, and without it is shown
by its position:

```rust
impl Sensor {
    fn new(builder: &mut Builder) -> Self {
        ports!(Self {
            node: builder.node(),
            reading: builder.output(),
        })
    }
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
`MessagePackSerializer` on the ports concerned. See `raio-rs/examples/scale.rs` and
`raio-py/examples/mixed.py`.

## Layout

```
raio-rs/src
  app/           what a node program is written with: App, ports, actions, reactions, connections
  node/          the scheduler that orders the events of one node by timestamp
  coordinator/   the graph of an app; wires its nodes at startup and shuts them down
  diagram/       draws an app in the style of Lingua Franca
  messages.rs    what crosses process boundaries
  serializer/    converting port values to and from bytes
  transport/     carrying bytes between processes
  runtime/       the single-threaded executor under each node
  driver/        its I/O and timers
raio-py
  src/           the Rust half of the binding: Python coroutines run as raio tasks
  python/raio/   the Python half: Node, ports, connect, run
```

## Developer Setup

### Setup Pre-Commit

```bash
pip install pre-commit
pre-commit install --hook-type commit-msg --hook-type pre-push
pre-commit run --all-files
```

### Build Project
#### Rust
```bash
cargo build
cargo test
```

#### Python
```bash
cd raio-py
maturin develop
python -m pytest tests/
```

### Publish package

#### Cargo
```bash
cargo publish -p raio-rs --dry-run
cargo publish -p raio-rs
```

#### PyPI
```bash
cd raio-py
maturin build --release
uv publish ../target/wheels/*
```
