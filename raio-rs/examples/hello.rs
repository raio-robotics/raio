use raio::app::{App, Builder, InputPort, OutputPort};

struct Producer {
    out: OutputPort<i64>,
}

impl Producer {
    fn new(builder: &mut Builder) -> Self {
        Self {
            out: builder.output(),
        }
    }

    async fn start(self) {
        println!("producer: hello from pid {}", std::process::id());
    }
}

struct Sink {
    input: InputPort<i64>,
}

impl Sink {
    fn new(builder: &mut Builder) -> Self {
        Self {
            input: builder.input(),
        }
    }

    async fn start(self) {
        println!("sink: hello from pid {}", std::process::id());
    }
}

fn main() {
    let mut app = App::new();
    let producer = app.create_node(Producer::new, Producer::start);
    let sink = app.create_node(Sink::new, Sink::start);
    app.connect(&producer.out, &sink.input);
    app.run();
}
