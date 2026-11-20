---
sidebar_position: 1
---

# Quick Start

<LanguageSelector />

## Install

Raio runs on Linux and macOS.

<Python>

Python 3.9 or later:

```bash
pip install raio-py
```

</Python>
<Rust>

A recent stable Rust toolchain:

```bash
cargo new hello && cd hello
cargo add raio-rs
```

</Rust>

## Hello World

<Python>

```python title="hello.py"
import raio
from raio import MS, InputPort, Node, OutputPort, reaction, task

# A node runs in its own process.
class Sender(Node):
    # Nodes communicate through ports.
    # An OutputPort can send values to InputPorts.
    out = OutputPort[str]()

    # When a node starts, its tasks are executed.
    @task
    async def send(self):
        await raio.wait(1 * MS)  # one millisecond of logical time
        self.out.set("hello world")  # send a value through the output port

class Listener(Node):
    # InputPorts receive values. [str] is a type annotation.
    inp = InputPort[str]()

    # A reaction runs each time its port receives a value.
    @reaction(inp)
    def show(self):
        print(self.inp.value)  # read input port values with .value

sender, listener = Sender(), Listener()
raio.connect(sender.out, listener.inp)  # port connections are type checked
raio.run(sender, listener)  # run the nodes until all of them finish
```

</Python>
<Rust>

```rust title="src/main.rs"
use raio::app::{App, Builder, InputPort, Node, OutputPort, Ports, node};
use raio::time::MS;

// A node runs in its own process.
#[derive(Ports)]
struct Sender {
    node: Node,
    // Nodes communicate through ports.
    // An OutputPort can send values to InputPorts.
    out: OutputPort<String>,
}

#[node]
impl Sender {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            out: builder.output(),
        }
    }

    // When a node starts, its tasks are executed.
    #[task]
    async fn send(&self) {
        self.node.wait(1 * MS).await; // one millisecond of logical time
        self.out.set("hello world".to_string()); // send a value through the output port
    }
}

#[derive(Ports)]
struct Listener {
    // InputPorts receive values. <String> is the type of the values.
    inp: InputPort<String>,
}

#[node]
impl Listener {
    fn new(builder: &mut Builder) -> Self {
        Self {
            inp: builder.input(),
        }
    }

    // A reaction runs each time its port receives a value.
    #[reaction(inp)]
    fn show(&self) {
        println!("{}", *self.inp.get().unwrap()); // read input port values with get()
    }
}

fn main() {
    let mut app = App::new();
    let sender = app.create_node(Sender::new);
    let listener = app.create_node(Listener::new);
    app.connect(&sender.out, &listener.inp); // port connections are type checked
    app.run(); // run the nodes until all of them finish
}
```

</Rust>

<Python>

```console
$ python hello.py
hello world
```

</Python>
<Rust>

```console
$ cargo run
hello world
```

</Rust>

## Add time

Implement your own timer with `await`:

<Python>

```python title="timed.py"
import raio
from raio import InputPort, Node, OutputPort, S, reaction, task

class TimerSender(Node):
    out = OutputPort[str]()

    @task
    async def send(self):
        for count in range(3):
            now = await raio.wait(1 * S)  # wait one second of logical time
            self.out.set(f"tick {count} at {now}")

# Listener is the same as before.
class Listener(Node):
    inp = InputPort[str]()

    @reaction(inp)
    def show(self):
        print(self.inp.value)

sender, listener = TimerSender(), Listener()
raio.connect(sender.out, listener.inp)
raio.run(sender, listener)
```

</Python>
<Rust>

```rust title="src/main.rs"
use raio::app::{App, Builder, InputPort, Node, OutputPort, Ports, node};
use raio::time::S;

#[derive(Ports)]
struct TimerSender {
    node: Node,
    out: OutputPort<String>,
}

#[node]
impl TimerSender {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            out: builder.output(),
        }
    }

    #[task]
    async fn send(&self) {
        for count in 0..3 {
            let now = self.node.wait(1 * S).await; // wait one second of logical time
            self.out.set(format!("tick {count} at {now}"));
        }
    }
}

// Listener is the same as before.
#[derive(Ports)]
struct Listener {
    inp: InputPort<String>,
}

#[node]
impl Listener {
    fn new(builder: &mut Builder) -> Self {
        Self {
            inp: builder.input(),
        }
    }

    #[reaction(inp)]
    fn show(&self) {
        println!("{}", *self.inp.get().unwrap());
    }
}

fn main() {
    let mut app = App::new();
    let sender = app.create_node(TimerSender::new);
    let listener = app.create_node(Listener::new);
    app.connect(&sender.out, &listener.inp);
    app.run();
}
```

</Rust>

<Python>

```console
$ python timed.py
tick 0 at 1s
tick 1 at 2s
tick 2 at 3s
```

</Python>
<Rust>

```console
$ cargo run
tick 0 at 1s
tick 1 at 2s
tick 2 at 3s
```

</Rust>

The timestamps are the same in every run.

<Python>

To skip the three-second wait, for example in a test:

```python
raio.run(sender, listener, fast_forward=True)  # same output, finishes at once
```

</Python>

## Next steps

- [More examples](https://github.com/raio-robotics/raio) in Python and Rust
- [Slack](https://join.slack.com/t/raio-hq/shared_invite/zt-4h90ce5q9-adb~cElnfDwekfEjcY0BtA) for questions
