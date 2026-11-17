use futures::join;
use raio::runtime::Runtime;

fn main() {
    let mut runtime = Runtime::new();

    runtime.block_on(async {
        println!("Hello, world! 1");
    });
    runtime.spawn(async {
        println!("Hello, world! 2");
    });
    runtime.spawn(async {
        println!("Hello, world! 3");
    });
    runtime.block_on(async {
        join!(
            async {
                println!("Hello, world! 4");
            },
            async {
                println!("Hello, world! 5");
            },
        );
    });
}
