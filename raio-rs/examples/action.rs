use raio::app::{Action, App, Builder, InputPort, Node, OutputPort};
use raio::tasks;
use std::io::{self, IsTerminal, Read};
use std::process::Command;
use std::rc::Rc;
use std::thread;

fn stty(settings: &[&str]) {
    if io::stdin().is_terminal() {
        assert!(
            Command::new("stty")
                .args(settings)
                .status()
                .unwrap()
                .success()
        );
    }
}

struct KeyboardInput {
    keys: Action<char>,
    out: OutputPort<char>,
}

impl KeyboardInput {
    fn new(builder: &mut Builder) -> Self {
        Self {
            keys: builder.action(),
            out: builder.output(),
        }
    }

    async fn start(self: Rc<Self>) {
        let handle = self.keys.handle();
        thread::spawn(move || {
            eprintln!("press keys; Ctrl-D to stop");
            stty(&["-icanon", "-echo", "min", "1"]);
            for byte in io::stdin().lock().bytes() {
                let byte = byte.unwrap();
                if byte == 0x04 {
                    break;
                }
                assert!(byte.is_ascii(), "this example reads ASCII keys only");
                handle.send(byte as char);
            }
            stty(&["icanon", "echo"]);
        });
        while let Some((key, _)) = self.keys.wait().await {
            self.out.set(*key);
        }
    }
}

struct Echo {
    node: Node,
    input: InputPort<char>,
}

impl Echo {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            input: builder.input(),
        }
    }

    async fn start(self: Rc<Self>) {
        while let Some((key, _)) = self.input.wait().await {
            println!("echo {} (physical: {})", *key, self.node.is_physical());
        }
    }
}

fn main() {
    let mut app = App::new();
    let keyboard = app.create_node(KeyboardInput::new, tasks![KeyboardInput::start]);
    let echo = app.create_node(Echo::new, tasks![Echo::start]);
    app.connect(&keyboard.out, &echo.input);
    app.run();
}
