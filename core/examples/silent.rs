use raio::app::{App, Builder, Node, Ports, node};
use raio::time::MS;
use std::time::Duration;

#[derive(Ports)]
struct Ticker {
    node: Node,
}

#[node]
impl Ticker {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
        }
    }

    #[task]
    async fn start(&self) {
        loop {
            self.node.wait(100 * MS).await;
        }
    }

    #[shutdown]
    fn shutdown(&self) {
        println!("ticker shut down");
    }
}

#[derive(Ports)]
struct Stuck {
    node: Node,
}

#[node]
impl Stuck {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
        }
    }

    #[task]
    async fn start(&self) {
        self.node.wait(1 * MS).await;
        // A task that blocks its thread keeps the node from sending its heartbeat.
        std::thread::sleep(Duration::from_secs(3600));
    }
}

fn main() {
    let mut app = App::new();
    app.create_node(Ticker::new);
    app.create_node(Stuck::new);
    // The coordinator gives up on the stuck node once it has been silent for the heartbeat
    // timeout, and shuts down the ticker.
    app.run();
}
