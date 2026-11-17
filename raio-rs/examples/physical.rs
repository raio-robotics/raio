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
        let mut count = 0;
        for _ in 0..3 {
            // Ping takes its time: it never waits for what Pong may still send.
            self.node.wait(1 * MS).await;
            self.out.set(count);
            let Some((reply, _)) = self.input.wait().await else {
                return;
            };
            count = *reply;
            println!("ping got {count} (physical: {})", self.node.is_physical());
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
    // Ping's clock stamps each reply when it arrives, so this loop needs no delay.
    app.connect(&pong.out, &ping.input).physical();
    app.run();
}
