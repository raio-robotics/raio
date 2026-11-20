---
sidebar_position: 4
---

# Drawing an App

<LanguageSelector />

A **diagram** is a picture of an app: its nodes, their ports, and the connections between them. Raio draws it from the app itself, so the picture always matches the code.

## Drawing a Diagram

<Python>

The program below builds an app of two nodes. In place of running it, **`raio.diagram`** draws it: the call takes the path of the file to write, here `hello.svg`, and the nodes of the app.

</Python>
<Rust>

The program below builds an app of two nodes. In place of running it, **`app.diagram`** draws it: the call takes the path of the file to write, here `hello.svg`.

</Rust>

<Python>

```python title="diagram.py" {21}
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
raio.connect(sender.out, listener.inp, delay=10 * MS)
raio.diagram("hello.svg", sender, listener)
```

</Python>
<Rust>

```rust title="src/main.rs" {52}
use raio::app::{App, Builder, InputPort, Node, OutputPort, Ports, node};
use raio::time::MS;

#[derive(Ports)]
struct Sender {
    node: Node,
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

    #[task]
    async fn send(&self) {
        self.node.wait(1 * MS).await;
        self.out.set("hello world".to_string());
    }
}

#[derive(Ports)]
struct Listener {
    node: Node,
    inp: InputPort<String>,
}

#[node]
impl Listener {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            inp: builder.input(),
        }
    }

    #[reaction(inp)]
    fn show(&self) {
        println!("{} at {}", *self.inp.get().unwrap(), self.node.now());
    }
}

fn main() {
    let mut app = App::new();
    let sender = app.create_node(Sender::new);
    let listener = app.create_node(Listener::new);
    app.connect(&sender.out, &listener.inp).delay(10 * MS);
    app.diagram("hello.svg");
}
```

</Rust>

The call draws the app and returns; it does not run it. A program can both draw and run its app. The file it writes is this picture:

![The Sender and Listener nodes, joined by a connection with a 10 ms delay](img/hello.svg)

## Reading a Diagram

In the diagram above:

- Each box is a node, shown by the name of its type. The number in its corner is the node's id: nodes are numbered from 1, in the order in which the program gives them to the app. An error message names a node by this id.
- A port is on the edge of its node, labelled with its name.
- A grey rectangle with rounded corners inside a node is a task, shown by the name of its method.
- A grey box with a point is a reaction. A line joins it to each input port or action that triggers it.
- The number in the corner of a task or reaction is its place, counted from 1, in the order in which the node declares them.
- A connection is a line from an output port to an input port. A delay is written on the line.
- A physical connection is a dashed line.

<Python>

A port has the name of its attribute on the node class.

</Python>
<Rust>

A port has the name of the field that holds it, which `#[derive(Ports)]` reads. A port that no field holds is shown by its position.

</Rust>

The nodes are placed and the connections routed by [elkrs](https://github.com/depetrol/elkrs).

Every program in this tutorial is shown with the diagram that Raio draws of it.
