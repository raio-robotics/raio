use raio::app::{App, Builder, InputPort, Node, OutputPort, Ports, node};
use raio::time::MS;

#[derive(Ports)]
struct Ticker {
    node: Node,
    out: OutputPort<i64>,
}

#[node]
impl Ticker {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            out: builder.output(),
        }
    }

    #[task]
    async fn start(&self) {
        let mut count = 0;
        loop {
            self.node.wait(1 * MS).await;
            self.out.set(count);
            count += 1;
        }
    }

    #[shutdown]
    fn shutdown(&self) {
        println!("ticker shut down");
    }
}

#[derive(Ports)]
struct Sink {
    input: InputPort<i64>,
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
            assert!(*value < 2, "the sink cannot take {}", *value);
            println!("sink got {}", *value);
        }
    }
}

fn main() {
    let mut app = App::new();
    let ticker = app.create_node(Ticker::new);
    let sink = app.create_node(Sink::new);
    app.connect(&ticker.out, &sink.input);
    // The sink fails at its third value. The coordinator then shuts down the ticker, which
    // would never finish on its own.
    app.run();
}
