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
struct Worker {
    node: Node,
    input: InputPort<i64>,
}

#[node]
impl Worker {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            input: builder.input(),
        }
    }

    #[task]
    async fn consume(&self) {
        while let Some((value, timestamp)) = self.input.wait().await {
            println!("consume {} at {}", *value, timestamp);
        }
    }

    #[task]
    async fn produce(&self) {
        for i in 0..4 {
            let now = self.node.wait(700 * MS).await;
            println!("produce {i} at {now}");
        }
    }
}

fn main() {
    let mut app = App::new();
    let ticker = app.create_node(|builder| Ticker::new(builder, 1 * S, 4));
    let worker = app.create_node(Worker::new);
    app.connect(&ticker.out, &worker.input);
    app.run();
}
