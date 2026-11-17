use std::process::Command;

fn run_example(name: &str) -> String {
    let mut path = std::env::current_exe().unwrap();
    path.pop();
    path.pop();
    path.push("examples");
    path.push(name);
    let output = Command::new(&path).output().unwrap();
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
    assert!(stdout.contains("producer: hello from pid"));
    assert!(stdout.contains("sink: hello from pid"));
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
