use raio::app::{App, Builder, InputPort, Node, OutputPort, Ports};
use raio::tasks;
use raio::time::MS;
use std::rc::Rc;

#[derive(Ports)]
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
        // The ticker runs until the user presses ^C.
        let mut count = 0;
        loop {
            self.node.wait(100 * MS).await;
            self.out.set(count);
            count += 1;
        }
    }

    fn shutdown(&self) {
        println!("ticker shut down");
    }
}

#[derive(Ports)]
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
        while let Some((value, time)) = self.input.wait().await {
            println!("sink got {} at {}", *value, time);
        }
    }

    fn shutdown(&self) {
        println!("sink shut down");
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
