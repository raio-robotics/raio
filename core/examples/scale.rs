use raio::app::{Builder, InputPort, OutputPort, Ports, create_node, run_node};
use raio::serializer::msgpack::MessagePackSerializer;
use raio::tasks;
use serde::{Deserialize, Serialize};
use std::rc::Rc;

/// A reading as a node in any language sees it: a map with these two entries.
#[derive(Serialize, Deserialize)]
struct Reading {
    name: String,
    value: f64,
}

#[derive(Ports)]
struct Scale {
    input: InputPort<Reading>,
    out: OutputPort<Reading>,
    factor: f64,
}

impl Scale {
    fn new(builder: &mut Builder, factor: f64) -> Self {
        Self {
            input: builder.input(),
            out: builder.output(),
            factor,
        }
    }

    async fn start(self: Rc<Self>) {
        while let Some((reading, _)) = self.input.wait().await {
            self.out.set(Reading {
                name: reading.name.clone(),
                value: reading.value * self.factor,
            });
        }
    }
}

/// Run only this node; a launcher starts the program and provides the rest of the app.
fn main() {
    let factor = std::env::args().nth(1).expect("usage: scale <factor>");
    let factor = factor.parse().expect("the factor is a number");
    let scale = create_node(|builder| Scale::new(builder, factor), tasks![Scale::start]);
    // With MessagePack the node's ports reach nodes written in Python as well.
    scale.input.set_serializer(MessagePackSerializer);
    scale.out.add_serializer(MessagePackSerializer);
    run_node(scale);
}
