use raio::app::{App, Builder, InputPort, Node, OutputPort};
use raio::tasks;
use raio::time::{S, Time};
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

struct Synchronizer {
    node: Node,
    left: InputPort<i64>,
    right: InputPort<i64>,
}

impl Synchronizer {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            left: builder.input(),
            right: builder.input(),
        }
    }

    async fn start(self: Rc<Self>) {
        while let Some((value, timestamp)) = self.left.wait().await {
            let Some(right) = self.right.get().map(|v| *v) else {
                continue;
            };
            println!(
                "{}: left {} and right {} at {}",
                self.node.now(),
                *value,
                right,
                timestamp
            );
        }
    }
}

fn main() {
    let mut app = App::new();
    let fast = app.create_node(
        Ticker::new,
        tasks![|ticker: Rc<Ticker>| ticker.start(1 * S, 6)],
    );
    let slow = app.create_node(
        Ticker::new,
        tasks![|ticker: Rc<Ticker>| ticker.start(1 * S, 3)],
    );
    let sync = app.create_node(Synchronizer::new, tasks![Synchronizer::start]);
    app.connect(&fast.out, &sync.left);
    app.connect(&slow.out, &sync.right);
    app.run();
}
