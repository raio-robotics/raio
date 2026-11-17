use raio::app::{App, Builder, InputPort, Node, OutputPort};
use raio::register;
use raio::time::{MS, S, Time};
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
}

impl Worker {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            input: builder.input(),
        }
    }

    async fn consume(self: Rc<Self>) {
        while let Some((value, timestamp)) = self.input.wait().await {
            println!("consume {} at {}", *value, timestamp);
        }
    }

    async fn produce(self: Rc<Self>) {
        for i in 0..4 {
            let now = self.node.wait(700 * MS).await;
            println!("produce {i} at {now}");
        }
    }
}

fn main() {
    let mut app = App::new();
    let ticker = app.create_node(
        Ticker::new,
        register![|ticker: Rc<Ticker>| ticker.start(1 * S, 4)],
    );
    let worker = app.create_node(Worker::new, register![Worker::consume, Worker::produce]);
    app.connect(&ticker.out, &worker.input);
    app.run();
}
