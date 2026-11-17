use futures::future::{Either, select};
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

struct Merge {
    node: Node,
    left: InputPort<i64>,
    right: InputPort<i64>,
}

impl Merge {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            left: builder.input(),
            right: builder.input(),
        }
    }

    async fn start(self: Rc<Self>) {
        loop {
            let closed = match select(self.left.pin(), self.right.pin()).await {
                Either::Left((value, _)) => value.is_none(),
                Either::Right((value, _)) => value.is_none(),
            };
            if closed {
                break;
            }
            println!(
                "{}: left {}, right {}",
                self.node.now(),
                self.left
                    .latest()
                    .map_or("none".to_string(), |(value, timestamp)| format!(
                        "{} at {}",
                        *value, timestamp
                    )),
                self.right
                    .latest()
                    .map_or("none".to_string(), |(value, timestamp)| format!(
                        "{} at {}",
                        *value, timestamp
                    )),
            );
        }
    }
}

fn main() {
    let mut app = App::new();
    let fast = app.create_node(
        Ticker::new,
        register![|ticker: Rc<Ticker>| ticker.start(900 * MS, 6)],
    );
    let slow = app.create_node(
        Ticker::new,
        register![|ticker: Rc<Ticker>| ticker.start(1 * S, 2)],
    );
    let merge = app.create_node(Merge::new, register![Merge::start]);
    app.connect(&fast.out, &merge.left);
    app.connect(&slow.out, &merge.right);
    app.run();
}
