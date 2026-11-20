use raio::app::{App, Builder, InputPort, Node, OutputPort, Ports, node};
use raio::time::MS;

#[derive(Ports)]
struct Ping {
    node: Node,
    input: InputPort<i64>,
    out: OutputPort<i64>,
}

#[node]
impl Ping {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            input: builder.input(),
            out: builder.output(),
        }
    }

    #[task]
    async fn start(&self) {
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

#[node]
impl Pong {
    fn new(builder: &mut Builder) -> Self {
        Self {
            input: builder.input(),
            out: builder.output(),
        }
    }

    #[task]
    async fn start(&self) {
        while let Some((count, _)) = self.input.wait().await {
            self.out.set(*count + 1);
        }
    }
}

fn main() {
    let mut app = App::new();
    let ping = app.create_node(Ping::new);
    let pong = app.create_node(Pong::new);
    app.connect(&ping.out, &pong.input);
    // Ping's clock stamps each reply when it arrives, so this loop needs no delay.
    app.connect(&pong.out, &ping.input).physical();
    app.run();
}
