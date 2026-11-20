use raio::app::{App, Builder, InputPort, Node, OutputPort, Ports, node};
use raio::time::{S, Time};

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
struct Synchronizer {
    node: Node,
    left: InputPort<i64>,
    right: InputPort<i64>,
}

#[node]
impl Synchronizer {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            left: builder.input(),
            right: builder.input(),
        }
    }

    #[task]
    async fn start(&self) {
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
    let fast = app.create_node(|builder| Ticker::new(builder, 1 * S, 6));
    let slow = app.create_node(|builder| Ticker::new(builder, 1 * S, 3));
    let sync = app.create_node(Synchronizer::new);
    app.connect(&fast.out, &sync.left);
    app.connect(&slow.out, &sync.right);
    app.run();
}
