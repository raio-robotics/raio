use raio::executor::create_executor;

fn main() {
    let (executor, spawner) = create_executor();
    spawner.spawn(async {
        println!("Hello, world! 1");
    });
    spawner.spawn(async {
        println!("Hello, world! 2");
    });
    drop(spawner);
    executor.run();
}