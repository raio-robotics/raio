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
