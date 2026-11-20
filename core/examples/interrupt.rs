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
        // The ticker runs until the user presses ^C.
        let mut count = 0;
        loop {
            self.node.wait(100 * MS).await;
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
        while let Some((value, time)) = self.input.wait().await {
            println!("sink got {} at {}", *value, time);
        }
    }

    #[shutdown]
    fn shutdown(&self) {
        println!("sink shut down");
    }
}

fn main() {
    let mut app = App::new();
    let ticker = app.create_node(Ticker::new);
    let sink = app.create_node(Sink::new);
    app.connect(&ticker.out, &sink.input);
    app.run();
}
