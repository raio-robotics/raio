use raio::app::{App, Builder, InputPort, Node, OutputPort, Ports, node};
use raio::time::MS;

#[derive(Ports)]
struct Ticker {
    node: Node,
    out: OutputPort<i64>,
}

#[node]
impl Ticker {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            out: builder.output(),
        }
    }

    #[task]
    async fn start(&self) {
        for i in 0..3 {
            self.node.wait(1 * MS).await;
            self.out.set(i);
        }
    }
}

#[derive(Ports)]
struct Sink {
    name: &'static str,
    input: InputPort<i64>,
}

#[node]
impl Sink {
    fn new(builder: &mut Builder, name: &'static str) -> Self {
        Self {
            name,
            input: builder.input(),
        }
    }

    #[task]
    async fn start(&self) {
        while let Some((value, timestamp)) = self.input.wait().await {
            println!("{} got {} at {}", self.name, *value, timestamp);
        }
    }
}

fn main() {
    let mut app = App::new();
    let ticker = app.create_node(Ticker::new);
    let near = app.create_node(|builder| Sink::new(builder, "near"));
    let far = app.create_node(|builder| Sink::new(builder, "far"));
    // One output feeds both inputs; the second connection adds a 10ms delay.
    app.connect(&ticker.out, &near.input);
    app.connect(&ticker.out, &far.input).delay(10 * MS);
    app.run();
}
