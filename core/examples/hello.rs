use raio::app::{App, Builder, InputPort, Node, OutputPort, Ports, node};
use raio::time::MS;

#[derive(Ports)]
struct Producer {
    node: Node,
    out: OutputPort<String>,
}

#[node]
impl Producer {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            out: builder.output(),
        }
    }

    #[task]
    async fn start(&self) {
        self.node.wait(1 * MS).await;
        self.out.set("hello world".to_string());
    }
}

#[derive(Ports)]
struct Sink {
    input: InputPort<String>,
}

#[node]
impl Sink {
    fn new(builder: &mut Builder) -> Self {
        Self {
            input: builder.input(),
        }
    }

    #[task]
    async fn start(&self) {
        while let Some((value, _)) = self.input.wait().await {
            println!("sink received {:?}", *value);
        }
    }
}

fn main() {
    let mut app = App::new();
    let producer = app.create_node(Producer::new);
    let sink = app.create_node(Sink::new);
    app.connect(&producer.out, &sink.input);
    app.run();
}
