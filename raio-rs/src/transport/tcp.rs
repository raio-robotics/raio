use crate::driver::AsyncFd;
use futures::io::{AsyncReadExt, AsyncWriteExt};
use std::io::ErrorKind;
use std::net;

use super::{Listener, Receiver, Sender, Transport};

pub struct TcpTransport;

pub struct TcpListener {
    listener: AsyncFd<net::TcpListener>,
    endpoint: String,
}

/// Share one nonblocking socket between a sender and a receiver.
fn split(stream: net::TcpStream) -> (TcpSender, TcpReceiver) {
    stream.set_nodelay(true).unwrap();
    stream.set_nonblocking(true).unwrap();
    let stream = AsyncFd::new(stream).unwrap();
    (
        TcpSender {
            stream: stream.clone(),
        },
        TcpReceiver { stream },
    )
}

impl Transport for TcpTransport {
    type Listener = TcpListener;
    type Sender = TcpSender;
    type Receiver = TcpReceiver;

    fn name(&self) -> &str {
        "tcp"
    }

    async fn bind(&self) -> TcpListener {
        let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        TcpListener {
            listener: AsyncFd::new(listener).unwrap(),
            endpoint,
        }
    }

    /// Block the runtime thread until the TCP handshake completes.
    async fn connect(&self, endpoint: &str) -> (TcpSender, TcpReceiver) {
        split(net::TcpStream::connect(endpoint).unwrap())
    }
}

impl Listener for TcpListener {
    type Sender = TcpSender;
    type Receiver = TcpReceiver;

    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    async fn accept(&mut self) -> (TcpSender, TcpReceiver) {
        let (stream, _) = self
            .listener
            .read_with(|listener| listener.accept())
            .await
            .unwrap();
        split(stream)
    }
}

pub struct TcpSender {
    stream: AsyncFd<net::TcpStream>,
}

impl Sender for TcpSender {
    async fn send(&mut self, msg: &[u8]) {
        // One write per message: a peer that has just exited still accepts it without an error.
        let mut frame = u32::try_from(msg.len()).unwrap().to_le_bytes().to_vec();
        frame.extend_from_slice(msg);
        self.stream.write_all(&frame).await.unwrap();
    }

    async fn call(&mut self, request: &[u8]) -> Vec<u8> {
        self.send(request).await;
        // Read the reply from the same TCP connection.
        read_frame(&mut self.stream)
            .await
            .expect("peer closed the connection without replying")
    }
}

pub struct TcpReceiver {
    stream: AsyncFd<net::TcpStream>,
}

impl Receiver for TcpReceiver {
    async fn recv(&mut self) -> Option<Vec<u8>> {
        read_frame(&mut self.stream).await
    }
}

async fn read_frame(stream: &mut AsyncFd<net::TcpStream>) -> Option<Vec<u8>> {
    let mut len = [0u8; 4];
    let n = match stream.read(&mut len[..1]).await {
        // A peer that exited with unread messages resets the connection.
        Err(error) if error.kind() == ErrorKind::ConnectionReset => 0,
        read => read.unwrap(),
    };
    if n == 0 {
        return None; // EOF before a new frame means the peer closed.
    }
    stream.read_exact(&mut len[1..]).await.unwrap();
    let mut msg = vec![0u8; u32::from_le_bytes(len) as usize];
    stream.read_exact(&mut msg).await.unwrap();
    Some(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::Runtime;

    #[test]
    fn frame_round_trip() {
        Runtime::new().block_on(async {
            let mut listener = TcpTransport.bind().await;
            let endpoint = listener.endpoint().to_string();
            let (mut sender, _) = TcpTransport.connect(&endpoint).await;
            let (_, mut receiver) = listener.accept().await;
            sender.send(b"hello").await;
            sender.send(b"").await; // Empty frames are valid messages.
            assert_eq!(receiver.recv().await.unwrap(), b"hello");
            assert_eq!(receiver.recv().await.unwrap(), b"");
            drop(sender);
            // A closed peer yields None after all frames are read.
            assert_eq!(receiver.recv().await, None);
        });
    }

    #[test]
    fn send_before_accept_is_buffered() {
        Runtime::new().block_on(async {
            let mut listener = TcpTransport.bind().await;
            let endpoint = listener.endpoint().to_string();
            let (mut sender, _) = TcpTransport.connect(&endpoint).await;
            // TCP buffers the data until the server accepts and reads it.
            sender.send(b"early").await;
            let (_, mut receiver) = listener.accept().await;
            assert_eq!(receiver.recv().await.unwrap(), b"early");
        });
    }

    #[test]
    fn all_byte_values_survive() {
        Runtime::new().block_on(async {
            let mut listener = TcpTransport.bind().await;
            let endpoint = listener.endpoint().to_string();
            let (mut sender, _) = TcpTransport.connect(&endpoint).await;
            let (_, mut receiver) = listener.accept().await;
            let msg: Vec<u8> = (0..=255).collect();
            sender.send(&msg).await;
            assert_eq!(receiver.recv().await.unwrap(), msg);
        });
    }

    #[test]
    fn ordering_across_many_frames() {
        Runtime::new().block_on(async {
            let mut listener = TcpTransport.bind().await;
            let endpoint = listener.endpoint().to_string();
            let (mut sender, _) = TcpTransport.connect(&endpoint).await;
            let (_, mut receiver) = listener.accept().await;
            futures::join!(
                async {
                    for i in 0..1000u32 {
                        sender.send(&i.to_le_bytes()).await;
                    }
                    drop(sender);
                },
                async {
                    for i in 0..1000u32 {
                        assert_eq!(receiver.recv().await.unwrap(), i.to_le_bytes());
                    }
                    assert_eq!(receiver.recv().await, None);
                },
            );
        });
    }

    #[test]
    fn frame_larger_than_socket_buffer() {
        Runtime::new().block_on(async {
            let mut listener = TcpTransport.bind().await;
            let endpoint = listener.endpoint().to_string();
            let (mut sender, _) = TcpTransport.connect(&endpoint).await;
            let (_, mut receiver) = listener.accept().await;
            let msg: Vec<u8> = (0..4 * 1024 * 1024u32).map(|i| i as u8).collect();
            let (_, received) = futures::join!(async { sender.send(&msg).await }, async {
                receiver.recv().await.unwrap()
            },);
            assert_eq!(received, msg);
        });
    }

    #[test]
    fn rpc_call_round_trip() {
        Runtime::new().block_on(async {
            let mut listener = TcpTransport.bind().await;
            let endpoint = listener.endpoint().to_string();
            let (mut client, _) = TcpTransport.connect(&endpoint).await;
            let (mut server_reply, mut server_rx) = listener.accept().await;
            let (response, _) = futures::join!(async { client.call(b"ping").await }, async {
                assert_eq!(server_rx.recv().await.unwrap(), b"ping");
                server_reply.send(b"pong").await;
            });
            assert_eq!(response, b"pong");
        });
    }

    #[test]
    fn connections_do_not_mix_frames() {
        Runtime::new().block_on(async {
            let mut listener = TcpTransport.bind().await;
            let endpoint = listener.endpoint().to_string();
            let (mut s1, _) = TcpTransport.connect(&endpoint).await;
            let (mut s2, _) = TcpTransport.connect(&endpoint).await;
            let (_, mut r1) = listener.accept().await;
            let (_, mut r2) = listener.accept().await;
            s1.send(b"from s1").await;
            s2.send(b"from s2").await;
            // Identify each connection by its payload rather than its accept order.
            let a = r1.recv().await.unwrap();
            let b = r2.recv().await.unwrap();
            assert!(
                (a == b"from s1" && b == b"from s2") || (a == b"from s2" && b == b"from s1"),
                "frames crossed connections: {a:?} / {b:?}"
            );
        });
    }
}
