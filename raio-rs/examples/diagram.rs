use raio::app::{App, Builder, InputPort, Node, OutputPort};
use raio::time::MS;
use raio::{ports, reaction, tasks};
use std::rc::Rc;

struct Sensor {
    node: Node,
    reading: OutputPort<i64>,
}

impl Sensor {
    fn new(builder: &mut Builder) -> Self {
        // A diagram shows each port by the name of its field.
        ports!(Self {
            node: builder.node(),
            reading: builder.output(),
        })
    }

    async fn sample(self: Rc<Self>) {
        for reading in 1..=3 {
            self.node.wait(1 * MS).await;
            self.reading.set(reading);
        }
    }
}

struct Controller {
    reading: InputPort<i64>,
    command: OutputPort<i64>,
}

impl Controller {
    fn new(builder: &mut Builder) -> Self {
        ports!(Self {
            reading: builder.input(),
            command: builder.output(),
        })
    }

    async fn control(self: Rc<Self>) {
        while let Some((reading, _)) = self.reading.wait().await {
            self.command.set(10 * *reading);
        }
    }
}

struct Motor {
    command: InputPort<i64>,
}

impl Motor {
    fn new(builder: &mut Builder) -> Self {
        ports!(Self {
            command: builder.input(),
        })
    }

    async fn drive(self: Rc<Self>) {
        while let Some((command, timestamp)) = self.command.wait().await {
            println!("motor drives {} at {timestamp}", *command);
        }
    }
}

struct Logger {
    reading: InputPort<i64>,
    command: InputPort<i64>,
}

impl Logger {
    fn new(builder: &mut Builder) -> Self {
        ports!(Self {
            reading: builder.input(),
            command: builder.input(),
        })
    }

    async fn log(self: Rc<Self>) {
        reaction!(self, [reading, command], {});
    }
}

fn main() {
    let mut app = App::new();
    let sensor = app.create_node(Sensor::new, tasks![Sensor::sample]);
    let controller = app.create_node(Controller::new, tasks![Controller::control]);
    let motor = app.create_node(Motor::new, tasks![Motor::drive]);
    let logger = app.create_node(Logger::new, tasks![Logger::log]);
    app.connect(&sensor.reading, &controller.reading);
    app.connect(&controller.command, &motor.command);
    app.connect(&sensor.reading, &logger.reading).delay(5 * MS);
    app.connect(&controller.command, &logger.command);
    // The picture goes to the file that the program's first argument names.
    let path = std::env::args().nth(1);
    app.diagram(path.unwrap_or("diagram.svg".to_string()));
    app.run();
}
