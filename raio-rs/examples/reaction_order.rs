use raio::app::{App, Builder, InputPort, Node, OutputPort, Ports};
use raio::reaction;
use raio::tasks;
use raio::time::{MS, Time};
use std::path::Path;
use std::rc::Rc;

#[derive(Ports)]
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

#[derive(Ports)]
struct Order {
    node: Node,
    a: InputPort<i64>,
    b: InputPort<i64>,
}

impl Order {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            a: builder.input(),
            b: builder.input(),
        }
    }

    async fn first(self: Rc<Self>) {
        reaction!(self, [b], {
            println!("{}: first, triggered by b", self.node.now());
        });
    }

    async fn second(self: Rc<Self>) {
        reaction!(self, [a], {
            println!("{}: second, triggered by a", self.node.now());
        });
    }

    async fn third(self: Rc<Self>) {
        reaction!(self, [a, b], {
            println!("{}: third, triggered by a and b", self.node.now());
        });
    }
}

fn main() {
    let mut app = App::new();
    let ticker = app.create_node(
        Ticker::new,
        tasks![|ticker: Rc<Ticker>| ticker.start(1 * MS, 2)],
    );
    // The reactions run in the order of this list, whichever port triggers them.
    let order = app.create_node(
        Order::new,
        tasks![Order::first, Order::second, Order::third],
    );
    app.connect(&ticker.out, &order.a);
    app.connect(&ticker.out, &order.b);
    app.diagram(Path::new("reaction_order.svg"));
    app.run();
}
