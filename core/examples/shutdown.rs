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
            if count == 3 {
                // Every node of the app shuts down at one timestamp.
                self.node.request_shutdown();
            }
            count += 1;
        }
    }

    #[shutdown]
    fn shutdown(&self) {
        println!("ticker shut down at {}", self.node.now());
    }
}

#[derive(Ports)]
struct Sink {
    node: Node,
    input: InputPort<i64>,
}

#[node]
impl Sink {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
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
        println!("sink shut down at {}", self.node.now());
    }
}

fn main() {
    let mut app = App::new();
    let ticker = app.create_node(Ticker::new);
    let sink = app.create_node(Sink::new);
    app.connect(&ticker.out, &sink.input);
    app.run();
}
