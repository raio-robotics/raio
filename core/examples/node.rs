use raio::app::{Builder, Node, OutputPort, Ports, create_node, node, run_node};
use raio::time::MS;

#[derive(Ports)]
struct Producer {
    node: Node,
    out: OutputPort<String>,
    text: String,
}

#[node]
impl Producer {
    fn new(builder: &mut Builder, text: String) -> Self {
        Self {
            node: builder.node(),
            out: builder.output(),
            text,
        }
    }

    #[task]
    async fn start(&self) {
        self.node.wait(1 * MS).await;
        self.out.set(self.text.clone());
    }
}

/// Run only this node; a launcher starts the program and provides the rest of the app.
fn main() {
    let text = std::env::args().nth(1).expect("usage: node <text>");
    let producer = create_node(|builder| Producer::new(builder, text));
    run_node(producer);
}
