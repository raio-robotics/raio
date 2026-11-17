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

    async fn start(self) {
        let Self { node, input } = self;

        let consume = async {
            while let Some((value, timestamp)) = input.wait().await {
                println!("consume {} at {}", *value, timestamp);
            }
        };

        let produce = async {
            for i in 0..4 {
                let now = node.wait(700 * MS).await;
                println!("produce {i} at {now}");
            }
        };

        futures::join!(consume, produce);
    }
}

fn main() {
    let mut app = App::new();
    let ticker = app.create_node(Ticker::new, |ticker| ticker.start(1 * S, 4));
    let worker = app.create_node(Worker::new, Worker::start);
    app.connect(&ticker.out, &worker.input);
    app.run();
}
