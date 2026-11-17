use raio::app::{App, Builder, Node};
use raio::tasks;
use raio::time::MS;
use std::rc::Rc;
use std::time::Duration;

struct Ticker {
    node: Node,
}

impl Ticker {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
        }
    }

    async fn start(self: Rc<Self>) {
        loop {
            self.node.wait(100 * MS).await;
        }
    }
}

struct Stuck {
    node: Node,
}

impl Stuck {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
        }
    }

    async fn start(self: Rc<Self>) {
        self.node.wait(1 * MS).await;
        // A task that blocks its thread keeps the node from sending its heartbeat.
        std::thread::sleep(Duration::from_secs(3600));
    }
}

fn main() {
    let mut app = App::new();
    app.create_node(Ticker::new, tasks![Ticker::start])
        .on_shutdown(|_| println!("ticker shut down"));
    app.create_node(Stuck::new, tasks![Stuck::start]);
    // The coordinator gives up on the stuck node once it has been silent for the heartbeat
    // timeout, and shuts down the ticker.
    app.run();
}
