use raio::app::{Assignment, Builder, Node, OutputPort, run_node};
use raio::register;
use raio::time::MS;
use std::rc::Rc;

struct Producer {
    node: Node,
    out: OutputPort<String>,
    text: String,
}

impl Producer {
    fn new(builder: &mut Builder, text: String) -> Self {
        Self {
            node: builder.node(),
            out: builder.output(),
            text,
        }
    }

    async fn start(self: Rc<Self>) {
        self.node.wait(1 * MS).await;
        self.out.set(self.text.clone());
    }
}

/// Run only this node; a launcher starts the program and provides the rest of the app.
fn main() {
    let assignment = Assignment::from_args().expect("a launcher starts this node");
    let text = std::env::args().nth(1).expect("usage: node <text>");
    run_node(
        assignment,
        |builder| Producer::new(builder, text),
        register![Producer::start],
    );
}
