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
            if count == 3 {
                // Every node of the app shuts down at one tag.
                self.node.request_shutdown();
            }
            count += 1;
        }
    }

    fn shutdown(&self) {
        println!("ticker shut down at {}", self.node.now());
    }
}

struct Sink {
    node: Node,
    input: InputPort<i64>,
}

impl Sink {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            input: builder.input(),
        }
    }

    async fn start(self: Rc<Self>) {
        while let Some((value, time)) = self.input.wait().await {
            println!("sink got {} at {}", *value, time);
        }
    }

    fn shutdown(&self) {
        println!("sink shut down at {}", self.node.now());
    }
}

fn main() {
    let mut app = App::new();
    let ticker = app
        .create_node(Ticker::new, tasks![Ticker::start])
        .on_shutdown(Ticker::shutdown);
    let sink = app
        .create_node(Sink::new, tasks![Sink::start])
        .on_shutdown(Sink::shutdown);
    app.connect(&ticker.out, &sink.input);
    app.run();
}
