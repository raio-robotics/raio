use raio::app::{App, Builder, InputPort, Node, OutputPort};
use raio::tasks;
use raio::time::{MS, S, Time};
use std::rc::Rc;

// Both tasks must finish before the node sends the tag's output.

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

    async fn start(self: Rc<Self>, period: Time, ticks: i64) {
        for i in 0..ticks {
            self.node.wait(period).await;
            self.out.set(i);
        }
    }
}

struct Worker {
    node: Node,
    input: InputPort<i64>,
    out: OutputPort<i64>,
}

impl Worker {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            input: builder.input(),
            out: builder.output(),
        }
    }

    async fn consume(self: Rc<Self>) {
        while self.input.wait().await.is_some() {}
    }

    async fn produce(self: Rc<Self>) {
        let now = self.node.wait(700 * MS).await;
        println!("worker sets 7 at {now}");
        self.out.set(7);
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
        while let Some((value, timestamp)) = self.input.wait().await {
            println!("sink got {} at {timestamp}", *value);
        }
    }
}

fn main() {
    let mut app = App::new();
    let ticker = app.create_node(
        Ticker::new,
        tasks![|ticker: Rc<Ticker>| ticker.start(1 * S, 2)],
    );
    let worker = app.create_node(Worker::new, tasks![Worker::consume, Worker::produce]);
    let sink = app.create_node(Sink::new, tasks![Sink::start]);
    app.connect(&ticker.out, &worker.input);
    app.connect(&worker.out, &sink.input);
    app.run();
}
