use std::io::Write;
use std::process::{Command, Stdio};

fn run_example(name: &str) -> String {
    run_example_with_input(name, "")
}

/// Run an example with `input` on its stdin, which the node processes inherit.
fn run_example_with_input(name: &str, input: &str) -> String {
    let mut path = std::env::current_exe().unwrap();
    path.pop();
    path.pop();
    path.push("examples");
    path.push(name);
    let mut child = Command::new(&path)
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
        .unwrap(); // dropped here: the readers see EOF after the input
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
fn same_tag() {
    let stdout = run_example("same_tag");
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
    // The result came in on the wall clock: the node resumed at a tag after 1s.
    assert!(nanos(node_time.trim()) > 1_000_000_000, "{node_time}");
}

/// Parse a `Time` as displayed: a number followed by one of its units.
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
