use futures::future::{Either, select};
use raio::app::{App, Builder, InputPort, Node, OutputPort};
use raio::time::{MS, S, Time};

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

    async fn start(self, period: Time, ticks: i64) {
        for i in 0..ticks {
            self.node.wait(period).await;
            self.out.set(i);
        }
    }
}

struct Merge {
    left: InputPort<i64>,
    right: InputPort<i64>,
}

impl Merge {
    fn new(builder: &mut Builder) -> Self {
        Self {
            left: builder.input(),
            right: builder.input(),
        }
    }

    async fn start(self) {
        loop {
            match select(self.left.pin(), self.right.pin()).await {
                Either::Left((Some((value, timestamp)), _)) => {
                    println!("left {} at {}", *value, timestamp);
                }
                Either::Right((Some((value, timestamp)), _)) => {
                    println!("right {} at {}", *value, timestamp);
                }
                Either::Left((None, _)) => {
                    println!("left closed");
                    break;
                }
                Either::Right((None, _)) => {
                    println!("right closed");
                    break;
                }
            }
        }
    }
}

fn main() {
    let mut app = App::new();
    let fast = app.create_node(Ticker::new, |ticker| ticker.start(900 * MS, 6));
    let slow = app.create_node(Ticker::new, |ticker| ticker.start(1 * S, 2));
    let merge = app.create_node(Merge::new, Merge::start);
    app.connect(&fast.out, &merge.left);
    app.connect(&slow.out, &merge.right);
    app.run();
}
