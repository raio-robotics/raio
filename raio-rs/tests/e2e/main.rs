mod tests;

use raio::app::{Assignment, Builder, InputPort, create_node, run_node_as};
use raio::coordinator::{Connection, Failure, Graph, PortId, spawn};
use raio::tasks;
use raio::time::Time;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::rc::Rc;

/// The program that cargo built from an example.
fn example(name: &str) -> PathBuf {
    let mut path = std::env::current_exe().unwrap();
    path.pop();
    path.pop();
    path.push("examples");
    path.push(name);
    path
}

fn run_example(name: &str) -> String {
    run_example_with_input(name, "")
}

/// Run an example process and supply its stdin from `input`.
fn run_example_with_input(name: &str, input: &str) -> String {
    let mut child = Command::new(example(name))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap(); // Dropping the pipe signals EOF to the child process.
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{name} exited with {}",
        output.status
    );
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn hello() {
    let stdout = run_example("hello");
    assert_eq!(stdout, "sink received \"hello world\"\n");
}

#[test]
fn timer() {
    let stdout = run_example("timer");
    assert_eq!(
        stdout,
        "tick 1 at 1s\n\
         tick 2 at 2s\n\
         tick 3 at 3s\n\
         tick 4 at 4s\n"
    );
}

#[test]
fn sync() {
    let stdout = run_example("sync");
    assert_eq!(
        stdout,
        "1s: left 0 and right 0 at 1s\n\
         2s: left 1 and right 1 at 2s\n\
         3s: left 2 and right 2 at 3s\n"
    );
}

#[test]
fn join() {
    let stdout = run_example("join");
    assert_eq!(
        stdout,
        "produce 0 at 700ms\n\
         consume 0 at 1s\n\
         produce 1 at 1400ms\n\
         consume 1 at 2s\n\
         produce 2 at 2100ms\n\
         produce 3 at 2800ms\n\
         consume 2 at 3s\n\
         consume 3 at 4s\n"
    );
}

#[test]
fn select() {
    let stdout = run_example("select");
    assert_eq!(
        stdout,
        "900ms: left 0 at 900ms, right none\n\
         1s: left 0 at 900ms, right 0 at 1s\n\
         1800ms: left 1 at 1800ms, right 0 at 1s\n\
         2s: left 1 at 1800ms, right 1 at 2s\n"
    );
}

#[test]
fn reaction() {
    let stdout = run_example("reaction");
    assert_eq!(
        stdout,
        "900ms: left 0 at 900ms, right none\n\
         1s: left 0 at 900ms, right 0 at 1s\n\
         1800ms: left 1 at 1800ms, right 0 at 1s\n\
         2s: left 1 at 1800ms, right 1 at 2s\n\
         2700ms: left 2 at 2700ms, right 1 at 2s\n\
         3600ms: left 3 at 3600ms, right 1 at 2s\n\
         4500ms: left 4 at 4500ms, right 1 at 2s\n\
         5400ms: left 5 at 5400ms, right 1 at 2s\n"
    );
}

#[test]
fn reaction_order() {
    let stdout = run_example("reaction_order");
    assert_eq!(
        stdout,
        "1ms: first, triggered by b\n\
         1ms: second, triggered by a\n\
         1ms: third, triggered by a and b\n\
         2ms: first, triggered by b\n\
         2ms: second, triggered by a\n\
         2ms: third, triggered by a and b\n"
    );
}

#[test]
fn same_timestamp() {
    let stdout = run_example("same_timestamp");
    assert_eq!(
        stdout,
        "worker sets 7 at 700ms\n\
         sink got 7 at 700ms\n"
    );
}

#[test]
fn action() {
    let stdout = run_example_with_input("action", "raio");
    assert_eq!(
        stdout,
        "echo r (physical: true)\n\
         echo a (physical: true)\n\
         echo i (physical: true)\n\
         echo o (physical: true)\n"
    );
}

#[test]
fn foreign() {
    let stdout = run_example("foreign");
    let (echoed, node_time) = stdout.rsplit_once("node time: ").unwrap();
    assert_eq!(
        echoed,
        "start work at 1s (physical: false)\n\
         got 42 (physical: true)\n"
    );
    // The foreign future must resume after the task's previous logical timestamp.
    assert!(nanos(node_time.trim()) > 1_000_000_000, "{node_time}");
}

#[test]
fn fanout() {
    let stdout = run_example("fanout");
    let mut lines: Vec<_> = stdout.lines().collect();
    lines.sort(); // The two sinks print from their own processes.
    assert_eq!(
        lines,
        [
            "far got 0 at 11ms",
            "far got 1 at 12ms",
            "far got 2 at 13ms",
            "near got 0 at 1ms",
            "near got 1 at 2ms",
            "near got 2 at 3ms",
        ]
    );
}

#[test]
fn serializer() {
    let stdout = run_example("serializer");
    assert_eq!(stdout, "sink received 42\n");
}

#[test]
fn shutdown() {
    let stdout = run_example("shutdown");
    let mut lines: Vec<_> = stdout.lines().collect();
    lines.sort(); // The two nodes print from their own processes.
    assert_eq!(
        lines,
        [
            "sink got 0 at 1ms",
            "sink got 1 at 2ms",
            "sink got 2 at 3ms",
            "sink got 3 at 4ms",
            // The ticker asked at 4ms; the tick at 5ms never ran.
            "sink shut down at (4ms, 1)",
            "ticker shut down at (4ms, 1)",
        ]
    );
}

#[test]
fn interrupt() {
    // The app gets a process group of its own, as it has in a terminal.
    let mut app = Command::new(example("interrupt"))
        .process_group(0)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut printed = BufReader::new(app.stdout.take().unwrap()).lines();
    assert_eq!(printed.next().unwrap().unwrap(), "sink got 0 at 100ms");

    // ^C in a terminal reaches the launcher and every node; only the launcher acts on it.
    // SAFETY: The signal goes to the process group that this test created.
    assert_eq!(unsafe { libc::kill(-(app.id() as i32), libc::SIGINT) }, 0);
    let mut rest: Vec<String> = printed.map(Result::unwrap).collect();
    assert!(app.wait().unwrap().success());
    // Each node ran its shutdown function at once, after at most the tick that was under way.
    rest.retain(|line| !line.starts_with("sink got"));
    rest.sort();
    assert_eq!(rest, ["sink shut down", "ticker shut down"]);
}

#[test]
fn failure() {
    let output = Command::new(example("failure")).output().unwrap();
    assert!(!output.status.success());
    // The coordinator shut the ticker down, so its shutdown function ran.
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "sink got 0\nsink got 1\nticker shut down\n"
    );
    let errors = String::from_utf8(output.stderr).unwrap();
    assert!(errors.contains("the sink cannot take 2"), "{errors}");
    // The launcher names the node that failed, not one that then shut down.
    assert!(errors.contains("node 1 exited with"), "{errors}");
    assert!(!errors.contains("node 0"), "{errors}");
}

#[test]
fn silent() {
    let output = Command::new(example("silent")).output().unwrap();
    assert!(!output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "ticker shut down\n"
    );
    let errors = String::from_utf8(output.stderr).unwrap();
    assert!(errors.contains("node 1 stopped responding"), "{errors}");
}

#[test]
fn diagram() {
    let path = std::env::temp_dir().join(format!("raio-diagram-{}.svg", std::process::id()));
    let output = Command::new(example("diagram"))
        .arg(&path)
        .output()
        .unwrap();
    assert!(output.status.success());
    // The app runs as it does without a diagram.
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "motor drives 10 at 1ms\nmotor drives 20 at 2ms\nmotor drives 30 at 3ms\n"
    );
    let svg = std::fs::read_to_string(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    assert!(svg.starts_with("<svg") && svg.ends_with("</svg>\n"));
    // A node has the name of its type, a task that of its method, and a port that of its field.
    let nodes = ["Sensor", "Controller", "Motor", "Logger"];
    let inside = ["sample", "control", "drive", "log", "reading", "command"];
    for name in nodes.into_iter().chain(inside) {
        assert!(
            svg.contains(&format!(">{name}</text>")),
            "{name} is missing"
        );
    }
    assert_eq!(svg.matches("class=\"connection\"").count(), 4);
    assert_eq!(svg.matches("class=\"port\"").count(), 6);
    assert!(svg.contains(">5ms</text>"));
}

/// Collects what the `node` example sends.
struct Collector {
    input: InputPort<String>,
    received: Rc<RefCell<Vec<String>>>,
}

impl Collector {
    async fn start(self: Rc<Self>) {
        while let Some((value, _)) = self.input.wait().await {
            self.received.borrow_mut().push(value.clone());
        }
    }
}

/// The results of the coordinator, the `node` example, and the collector.
type Launched = (
    Result<(), Failure>,
    Output,
    std::thread::Result<Vec<String>>,
);

/// Run the `node` example and an in-process collector, connected at the collector's `input` port.
fn launch(input: usize) -> Launched {
    let (from, to) = (
        PortId {
            node_id: 0,
            port_id: 0,
        },
        PortId {
            node_id: 1,
            port_id: input,
        },
    );
    let graph = Graph {
        nodes: 2,
        connections: vec![Connection {
            from,
            to,
            delay: Time::ZERO,
        }],
    };
    let (coordinator_endpoint, coordinator, _) = spawn(graph, HashMap::new());
    let assign = |node_id| Assignment {
        node_id,
        coordinator_endpoint: coordinator_endpoint.clone(),
    };

    let example = Command::new(example("node"))
        .arg("hello world")
        .args(assign(0).args())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let assignment = assign(1);
    let collector = std::thread::spawn(move || {
        let received = Rc::new(RefCell::new(Vec::new()));
        let seen = received.clone();
        let constructor = move |builder: &mut Builder| Collector {
            input: builder.input(),
            received: seen,
        };
        run_node_as(
            assignment,
            create_node(constructor, tasks![Collector::start]),
        );
        received.take()
    });
    (
        coordinator.join().unwrap(),
        example.wait_with_output().unwrap(),
        collector.join(),
    )
}

#[test]
fn a_node_in_its_own_program_is_wired_by_port_position() {
    let (wired, example, collected) = launch(0);
    assert_eq!(wired, Ok(()));
    assert!(example.status.success());
    assert_eq!(collected.unwrap(), ["hello world"]);
}

#[test]
fn a_graph_that_names_a_missing_port_stops_every_node() {
    let (wired, example, collected) = launch(3);
    let refusal = "node 1 has no input port 3";
    assert_eq!(wired, Err(Failure::Refused(refusal.to_string())));
    assert!(!example.status.success());
    assert!(String::from_utf8(example.stderr).unwrap().contains(refusal));
    let panic = collected.unwrap_err();
    assert!(panic.downcast_ref::<String>().unwrap().contains(refusal));
}

/// Parse a displayed time without microsteps into nanoseconds.
fn nanos(time: &str) -> i64 {
    let digits = time.trim_end_matches(|c: char| c.is_ascii_alphabetic());
    let scale = match &time[digits.len()..] {
        "ns" => 1,
        "us" => 1_000,
        "ms" => 1_000_000,
        "s" => 1_000_000_000,
        unit => panic!("unexpected unit {unit:?} in {time:?}"),
    };
    digits.parse::<i64>().unwrap() * scale
}
