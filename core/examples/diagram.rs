use raio::app::{App, Builder, InputPort, Node, OutputPort, Ports, node};
use raio::time::MS;

// A diagram shows each port by the name of the field that holds it.
#[derive(Ports)]
struct Sensor {
    node: Node,
    reading: OutputPort<i64>,
}

#[node]
impl Sensor {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            reading: builder.output(),
        }
    }

    #[task]
    async fn sample(&self) {
        for reading in 1..=3 {
            self.node.wait(1 * MS).await;
            self.reading.set(reading);
        }
    }
}

#[derive(Ports)]
struct Controller {
    reading: InputPort<i64>,
    command: OutputPort<i64>,
}

#[node]
impl Controller {
    fn new(builder: &mut Builder) -> Self {
        Self {
            reading: builder.input(),
            command: builder.output(),
        }
    }

    #[task]
    async fn control(&self) {
        while let Some((reading, _)) = self.reading.wait().await {
            self.command.set(10 * *reading);
        }
    }
}

#[derive(Ports)]
struct Motor {
    command: InputPort<i64>,
}

#[node]
impl Motor {
    fn new(builder: &mut Builder) -> Self {
        Self {
            command: builder.input(),
        }
    }

    #[task]
    async fn drive(&self) {
        while let Some((command, timestamp)) = self.command.wait().await {
            println!("motor drives {} at {timestamp}", *command);
        }
    }
}

#[derive(Ports)]
struct Logger {
    reading: InputPort<i64>,
    command: InputPort<i64>,
}

#[node]
impl Logger {
    fn new(builder: &mut Builder) -> Self {
        Self {
            reading: builder.input(),
            command: builder.input(),
        }
    }

    #[reaction(reading, command)]
    fn log(&self) {}
}

fn main() {
    let mut app = App::new();
    let sensor = app.create_node(Sensor::new);
    let controller = app.create_node(Controller::new);
    let motor = app.create_node(Motor::new);
    let logger = app.create_node(Logger::new);
    app.connect(&sensor.reading, &controller.reading);
    app.connect(&controller.command, &motor.command);
    app.connect(&sensor.reading, &logger.reading).delay(5 * MS);
    app.connect(&controller.command, &logger.command);
    // The picture goes to the file that the program's first argument names.
    let path = std::env::args().nth(1);
    app.diagram(path.unwrap_or("diagram.svg".to_string()));
    app.run();
}
