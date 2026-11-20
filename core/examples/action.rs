use raio::app::{Action, App, Builder, InputPort, Node, OutputPort, Ports, node};
use std::io::{self, IsTerminal, Read};
use std::process::Command;
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

#[derive(Ports)]
struct KeyboardInput {
    keys: Action<char>,
    out: OutputPort<char>,
}

#[node]
impl KeyboardInput {
    fn new(builder: &mut Builder) -> Self {
        Self {
            keys: builder.action(),
            out: builder.output(),
        }
    }

    #[task]
    async fn start(&self) {
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

#[derive(Ports)]
struct Echo {
    node: Node,
    input: InputPort<char>,
}

#[node]
impl Echo {
    fn new(builder: &mut Builder) -> Self {
        Self {
            node: builder.node(),
            input: builder.input(),
        }
    }

    #[task]
    async fn start(&self) {
        while let Some((key, _)) = self.input.wait().await {
            println!("echo {} (physical: {})", *key, self.node.is_physical());
        }
    }
}

fn main() {
    let mut app = App::new();
    let keyboard = app.create_node(KeyboardInput::new);
    let echo = app.create_node(Echo::new);
    app.connect(&keyboard.out, &echo.input);
    app.run();
}
