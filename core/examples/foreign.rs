use raio::app::{App, Builder, Node, Ports, node};
use raio::time::S;
use std::time::Duration;

#[derive(Ports)]
struct Worker {
    node: Node,
}

#[node]
impl Worker {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
        }
    }

    #[task]
    async fn start(&self) {
        self.node.wait(1 * S).await;
        println!(
            "start work at {} (physical: {})",
            self.node.now(),
            self.node.is_physical()
        );
        // Simulate work completed by another runtime.
        async_io::Timer::after(Duration::from_millis(30)).await;
        println!("got 42 (physical: {})", self.node.is_physical());
        println!("node time: {}", self.node.now());
    }
}

fn main() {
    let mut app = App::new();
    app.create_node(Worker::new);
    app.run();
}
