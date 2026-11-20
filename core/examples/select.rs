use futures::future::{Either, select};
use raio::app::{App, Builder, InputPort, Node, OutputPort, Ports, node};
use raio::time::{MS, S, Time};

#[derive(Ports)]
struct Ticker {
    node: Node,
    out: OutputPort<i64>,
    period: Time,
    ticks: i64,
}

#[node]
impl Ticker {
    fn new(builder: &mut Builder, period: Time, ticks: i64) -> Self {
        Self {
            node: builder.node(),
            out: builder.output(),
            period,
            ticks,
        }
    }

    #[task]
    async fn start(&self) {
        for i in 0..self.ticks {
            self.node.wait(self.period).await;
            self.out.set(i);
        }
    }
}

#[derive(Ports)]
struct Merge {
    node: Node,
    left: InputPort<i64>,
    right: InputPort<i64>,
}

#[node]
impl Merge {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            left: builder.input(),
            right: builder.input(),
        }
    }

    #[task]
    async fn start(&self) {
        loop {
            let closed = match select(self.left.wait(), self.right.wait()).await {
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
    let fast = app.create_node(|builder| Ticker::new(builder, 900 * MS, 6));
    let slow = app.create_node(|builder| Ticker::new(builder, 1 * S, 2));
    let merge = app.create_node(Merge::new);
    app.connect(&fast.out, &merge.left);
    app.connect(&slow.out, &merge.right);
    app.run();
}
