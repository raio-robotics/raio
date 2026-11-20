---
sidebar_position: 2
---

# Rust API

The crate is published as `raio-rs` and imported as `raio`. Its complete API is documented on [docs.rs](https://docs.rs/raio-rs).

```bash
cargo add raio-rs
```

```rust title="core/examples/hello.rs"
use raio::app::{App, Builder, InputPort, Node, OutputPort, Ports, node};
use raio::time::MS;

#[derive(Ports)]
struct Producer {
    node: Node,
    out: OutputPort<String>,
}

#[node]
impl Producer {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            out: builder.output(),
        }
    }

    #[task]
    async fn start(&self) {
        self.node.wait(1 * MS).await;
        self.out.set("hello world".to_string());
    }
}

#[derive(Ports)]
struct Sink {
    input: InputPort<String>,
}

#[node]
impl Sink {
    fn new(builder: &mut Builder) -> Self {
        Self {
            input: builder.input(),
        }
    }

    #[task]
    async fn start(&self) {
        while let Some((value, _)) = self.input.wait().await {
            println!("sink received {:?}", *value);
        }
    }
}

fn main() {
    let mut app = App::new();
    let producer = app.create_node(Producer::new);
    let sink = app.create_node(Sink::new);
    app.connect(&producer.out, &sink.input);
    app.run();
}
```

A node is a struct that derives `Ports`, and a function that takes a `Builder` builds it. Its `impl` block is marked `#[node]`, and the methods in it that are marked `#[task]`, `#[reaction(...)]` or `#[shutdown]` are what the node runs. `App::create_node` takes the function that builds the node, `App::connect` joins two ports, and `App::run` starts one process per node.

```console
$ cargo run --example hello
sink received "hello world"
```

## Rust and Python Side by Side

The two APIs have the same concepts under slightly different names:

| | Rust | Python |
|---|---|---|
| Wait in logical time | `node.wait(10 * MS).await` | `await raio.wait(10 * MS)` |
| Next value of an input | `input.wait().await` | `await port`, `async for value in port` |
| Value at the current timestamp | `input.get()` | `port.value` when `port.present` |
| Set an output | `out.set(value)` | `out.set(value)` |
| Declare a task | `#[task]` on an `async` method | `@task` on an `async` method |
| React to inputs | `#[reaction(a, b)]` on a method | `@reaction(a, b)` on a method |
| Wait for several things | `select` and `join!` of the `futures` crate | `raio.select`, `raio.gather`, `raio.timeout` |
| Start another task | `node.spawn(future)` | `raio.spawn(coroutine)` |
| Current timestamp | `node.now()` | `raio.now()` |
| Events from threads | `Action<T>` and its `handle()` | `Action[T]` and its `send` |
| Connect | `app.connect(&a.out, &b.input).delay(d)` | `raio.connect(a.out, b.inp, delay=d)` |
| Connect physically | `app.connect(&a.out, &b.input).physical()` | `raio.connect(a.out, b.inp, physical=True)` |
| Shut the app down | `node.request_shutdown()` | `raio.request_shutdown()` |
| Run at shutdown | `#[shutdown]` on a method | `def shutdown(self)` |
| Draw the app | `app.diagram("app.svg")` | `raio.diagram("app.svg", a, b)` |

## Differences from Python

Port values are serialized with bincode by default, where Python uses pickle. A port that reaches a Python node takes `MessagePackSerializer`; see [Rust and Python in One App](../tutorial/rust-and-python.md).

A Rust task may await a future that Raio does not provide, such as one of another runtime. Its result arrives as a physical event, like a value sent to an action.

The Rust `App` has no fast-forward mode.

The version of the crate is `raio::VERSION`.
