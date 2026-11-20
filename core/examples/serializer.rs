use raio::app::{App, Builder, InputPort, Node, OutputPort, Ports, node};
use raio::serializer::Serializer;
use raio::time::MS;

/// Sends a number as decimal text.
struct Text;

impl Serializer<i64> for Text {
    fn serialize(&self, value: &i64) -> Vec<u8> {
        value.to_string().into_bytes()
    }

    fn deserialize(&self, bytes: &[u8]) -> i64 {
        std::str::from_utf8(bytes).unwrap().parse().unwrap()
    }
}

#[derive(Ports)]
struct Producer {
    node: Node,
    out: OutputPort<i64>,
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
        self.out.set(42);
    }
}

#[derive(Ports)]
struct Sink {
    input: InputPort<i64>,
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
            println!("sink received {}", *value);
        }
    }
}

fn main() {
    let mut app = App::new();
    let producer = app.create_node(Producer::new);
    let sink = app.create_node(Sink::new);
    // The connection uses its own serializer in place of bincode.
    app.connect(&producer.out, &sink.input).serializer(Text);
    app.run();
}
