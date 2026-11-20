use raio::app::{App, Builder, InputPort, Node, OutputPort, Ports, node};
use raio::time::{MS, S, Time};

// Both tasks must finish before the node sends the timestamp's output.

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
    out: OutputPort<i64>,
}

#[node]
impl Worker {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            input: builder.input(),
            out: builder.output(),
        }
    }

    #[task]
    async fn consume(&self) {
        while self.input.wait().await.is_some() {}
    }

    #[task]
    async fn produce(&self) {
        let now = self.node.wait(700 * MS).await;
        println!("worker sets 7 at {now}");
        self.out.set(7);
    }
}

#[derive(Ports)]
struct Sink {
    input: InputPort<i64>,
}

#[node]
impl Sink {
    fn new(builder: &mut Builder) -> Self {
        Self {
            input: builder.input(),
        }
    }

    #[task]
    async fn start(&self) {
        while let Some((value, timestamp)) = self.input.wait().await {
            println!("sink got {} at {timestamp}", *value);
        }
    }
}

fn main() {
    let mut app = App::new();
    let ticker = app.create_node(|builder| Ticker::new(builder, 1 * S, 2));
    let worker = app.create_node(Worker::new);
    let sink = app.create_node(Sink::new);
    app.connect(&ticker.out, &worker.input);
    app.connect(&worker.out, &sink.input);
    app.run();
}
