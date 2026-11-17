use raio::app::{App, Builder, InputPort, Node, OutputPort};
use raio::tasks;
use raio::time::MS;
use std::rc::Rc;

struct Ticker {
    node: Node,
    out: OutputPort<i64>,
}

impl Ticker {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            out: builder.output(),
        }
    }

    async fn start(self: Rc<Self>) {
        for i in 0..3 {
            self.node.wait(1 * MS).await;
            self.out.set(i);
        }
    }
}

struct Sink {
    name: &'static str,
    input: InputPort<i64>,
}

impl Sink {
    fn new(builder: &mut Builder, name: &'static str) -> Self {
        Self {
            name,
            input: builder.input(),
        }
    }

    async fn start(self: Rc<Self>) {
        while let Some((value, timestamp)) = self.input.wait().await {
            println!("{} got {} at {}", self.name, *value, timestamp);
        }
    }
}

fn main() {
    let mut app = App::new();
    let ticker = app.create_node(Ticker::new, tasks![Ticker::start]);
    let near = app.create_node(|builder| Sink::new(builder, "near"), tasks![Sink::start]);
    let far = app.create_node(|builder| Sink::new(builder, "far"), tasks![Sink::start]);
    // One output feeds both inputs; the second connection adds a 10ms delay.
    app.connect(&ticker.out, &near.input);
    app.connect(&ticker.out, &far.input).delay(10 * MS);
    app.run();
}
