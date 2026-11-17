use raio::app::{App, Builder, InputPort, Node, OutputPort, Ports};
use raio::tasks;
use raio::time::MS;
use std::rc::Rc;

#[derive(Ports)]
struct Ping {
    node: Node,
    input: InputPort<i64>,
    out: OutputPort<i64>,
}

impl Ping {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            input: builder.input(),
            out: builder.output(),
        }
    }

    async fn start(self: Rc<Self>) {
        // The loop starts here, before the delay of the connection back has passed.
        self.node.wait(1 * MS).await;
        let mut count = 0;
        for _ in 0..3 {
            self.out.set(count);
            let Some((reply, _)) = self.input.wait().await else {
                return;
            };
            count = *reply;
            println!("ping got {count} at {}", self.node.now());
        }
    }
}

#[derive(Ports)]
struct Pong {
    input: InputPort<i64>,
    out: OutputPort<i64>,
}

impl Pong {
    fn new(builder: &mut Builder) -> Self {
        Self {
            input: builder.input(),
            out: builder.output(),
        }
    }

    async fn start(self: Rc<Self>) {
        while let Some((count, _)) = self.input.wait().await {
            self.out.set(*count + 1);
        }
    }
}

fn main() {
    let mut app = App::new();
    let ping = app.create_node(Ping::new, tasks![Ping::start]);
    let pong = app.create_node(Pong::new, tasks![Pong::start]);
    app.connect(&ping.out, &pong.input);
    // A loop of connections needs a delay.
    app.connect(&pong.out, &ping.input).delay(10 * MS);
    app.run();
}
