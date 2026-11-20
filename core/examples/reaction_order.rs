use raio::app::{App, Builder, InputPort, Node, OutputPort, Ports, node};
use raio::time::{MS, Time};
use std::path::Path;

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
struct Order {
    node: Node,
    a: InputPort<i64>,
    b: InputPort<i64>,
}

#[node]
impl Order {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            a: builder.input(),
            b: builder.input(),
        }
    }

    #[reaction(b)]
    fn first(&self) {
        println!("{}: first, triggered by b", self.node.now());
    }

    #[reaction(a)]
    fn second(&self) {
        println!("{}: second, triggered by a", self.node.now());
    }

    #[reaction(a, b)]
    fn third(&self) {
        println!("{}: third, triggered by a and b", self.node.now());
    }
}

fn main() {
    let mut app = App::new();
    let ticker = app.create_node(|builder| Ticker::new(builder, 1 * MS, 2));
    // The reactions run in the order in which `Order` declares them, whichever port triggers
    // them.
    let order = app.create_node(Order::new);
    app.connect(&ticker.out, &order.a);
    app.connect(&ticker.out, &order.b);
    app.diagram(Path::new("reaction_order.svg"));
    app.run();
}
