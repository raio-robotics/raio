use async_net::TcpListener;
use futures::io::{AsyncReadExt, AsyncWriteExt};
use raio::runtime::Runtime;

fn main() {
    let runtime = Runtime::new();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:8000").await.unwrap();
        println!("listening on 127.0.0.1:8000");
        loop {
            let (mut stream, addr) = listener.accept().await.unwrap();
            println!("connection from {addr}");
            let mut buf = [0u8; 1024];
            loop {
                let n = stream.read(&mut buf).await.unwrap();
                if n == 0 {
                    break; // The peer closed the connection.
                }
                println!("received data: {:?}", String::from_utf8_lossy(&buf[..n]));
                stream.write_all(&buf[..n]).await.unwrap();
            }
            println!("connection closed");
        }
    });
}
