use raio::app::{App, Builder, Node, Ports, node};
use raio::time::S;

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
        for i in 1..5 {
            let now = self.node.wait(1 * S).await;
            println!("tick {i} at {now}");
        }
    }
}

fn main() {
    let mut app = App::new();
    app.create_node(Ticker::new);
    app.run();
}
