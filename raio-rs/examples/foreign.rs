use raio::app::{App, Builder, Node};
use raio::register;
use raio::time::S;
use std::rc::Rc;
use std::time::Duration;

struct Worker {
    node: Node,
}

impl Worker {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
        }
    }

    async fn start(self: Rc<Self>) {
        let now = self.node.wait(1 * S).await;
        println!(
            "start work at {} (physical: {})",
            self.node.now(),
            self.node.physical()
        );
        async_io::Timer::after(Duration::from_millis(30)).await; // the work
        println!("got 42 (physical: {})", self.node.physical());
        println!("node time: {}", self.node.now());
    }
}

fn main() {
    let mut app = App::new();
    app.create_node(Worker::new, register![Worker::start]);
    app.run();
}
