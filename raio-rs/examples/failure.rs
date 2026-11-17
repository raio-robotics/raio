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
        let mut count = 0;
        loop {
            self.node.wait(1 * MS).await;
            self.out.set(count);
            count += 1;
        }
    }
}

struct Sink {
    input: InputPort<i64>,
}

impl Sink {
    fn new(builder: &mut Builder) -> Self {
        Self {
            input: builder.input(),
        }
    }

    async fn start(self: Rc<Self>) {
        while let Some((value, _)) = self.input.wait().await {
            assert!(*value < 2, "the sink cannot take {}", *value);
            println!("sink got {}", *value);
        }
    }
}

fn main() {
    let mut app = App::new();
    let ticker = app
        .create_node(Ticker::new, tasks![Ticker::start])
        .on_shutdown(|_| println!("ticker shut down"));
    let sink = app.create_node(Sink::new, tasks![Sink::start]);
    app.connect(&ticker.out, &sink.input);
    // The sink fails at its third value. The coordinator then shuts down the ticker, which
    // would never finish on its own.
    app.run();
}
