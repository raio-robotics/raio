use async_net::{TcpListener, TcpStream};
use futures::io::{AsyncReadExt, AsyncWriteExt};

use super::{Receiver, Sender, Transport};

pub struct TcpTransport {
    listener: TcpListener,
    endpoint: String,
}

impl Transport for TcpTransport {
    type Sender = TcpSender;
    type Receiver = TcpReceiver;

    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        Self { listener, endpoint }
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    async fn accept(&mut self) -> (TcpSender, TcpReceiver) {
        let (stream, _) = self.listener.accept().await.unwrap();
        stream.set_nodelay(true).unwrap();
        // Two handles to one socket: writes go through one, reads the other.
        (
            TcpSender {
                stream: stream.clone(),
            },
            TcpReceiver { stream },
        )
    }

    async fn connect(&self, endpoint: &str) -> (TcpSender, TcpReceiver) {
        let stream = TcpStream::connect(endpoint).await.unwrap();
        stream.set_nodelay(true).unwrap();
        (
            TcpSender {
                stream: stream.clone(),
            },
            TcpReceiver { stream },
        )
    }
}

pub struct TcpSender {
    stream: TcpStream,
}

impl Sender for TcpSender {
    async fn send(&mut self, msg: &[u8]) {
        let len = u32::try_from(msg.len()).unwrap().to_le_bytes();
        self.stream.write_all(&len).await.unwrap();
        self.stream.write_all(msg).await.unwrap();
    }

    async fn call(&mut self, request: &[u8]) -> Vec<u8> {
        self.send(request).await;
        // TCP is full duplex: the reply arrives on this same stream.
        read_frame(&mut self.stream)
            .await
            .expect("peer closed the connection without replying")
    }
}

pub struct TcpReceiver {
    stream: TcpStream,
}

impl Receiver for TcpReceiver {
    async fn recv(&mut self) -> Option<Vec<u8>> {
        read_frame(&mut self.stream).await
    }
}

async fn read_frame(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut len = [0u8; 4];
    let n = stream.read(&mut len[..1]).await.unwrap();
    if n == 0 {
        return None; // EOF at a frame boundary: peer closed cleanly
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
            let mut t = TcpTransport::new().await;
            let endpoint = t.endpoint().to_string();
            let (mut sender, _) = t.connect(&endpoint).await;
            let (_, mut receiver) = t.accept().await;
            sender.send(b"hello").await;
            sender.send(b"").await; // empty frame is a legal message
            assert_eq!(receiver.recv().await.unwrap(), b"hello");
            assert_eq!(receiver.recv().await.unwrap(), b"");
            drop(sender);
            assert_eq!(receiver.recv().await, None); // close propagates as None
        });
    }

    #[test]
    fn send_before_accept_is_buffered() {
        Runtime::new().block_on(async {
            let mut t = TcpTransport::new().await;
            let endpoint = t.endpoint().to_string();
            let (mut sender, _) = t.connect(&endpoint).await;
            sender.send(b"early").await; // kernel buffers it in the backlog
            let (_, mut receiver) = t.accept().await;
            assert_eq!(receiver.recv().await.unwrap(), b"early");
        });
    }

    #[test]
    fn all_byte_values_survive() {
        Runtime::new().block_on(async {
            let mut t = TcpTransport::new().await;
            let endpoint = t.endpoint().to_string();
            let (mut sender, _) = t.connect(&endpoint).await;
            let (_, mut receiver) = t.accept().await;
            let msg: Vec<u8> = (0..=255).collect();
            sender.send(&msg).await;
            assert_eq!(receiver.recv().await.unwrap(), msg);
        });
    }

    #[test]
    fn ordering_across_many_frames() {
        Runtime::new().block_on(async {
            let mut t = TcpTransport::new().await;
            let endpoint = t.endpoint().to_string();
            let (mut sender, _) = t.connect(&endpoint).await;
            let (_, mut receiver) = t.accept().await;
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
            let mut t = TcpTransport::new().await;
            let endpoint = t.endpoint().to_string();
            let (mut sender, _) = t.connect(&endpoint).await;
            let (_, mut receiver) = t.accept().await;
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
            let mut t = TcpTransport::new().await;
            let endpoint = t.endpoint().to_string();
            let (mut client, _) = t.connect(&endpoint).await;
            let (mut server_reply, mut server_rx) = t.accept().await;
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
            let mut t = TcpTransport::new().await;
            let endpoint = t.endpoint().to_string();
            let (mut s1, _) = t.connect(&endpoint).await;
            let (mut s2, _) = t.connect(&endpoint).await;
            let (_, mut r1) = t.accept().await;
            let (_, mut r2) = t.accept().await;
            s1.send(b"from s1").await;
            s2.send(b"from s2").await;
            // accept order is not part of the contract, so match by content:
            // each connection carries exactly its own sender's frame.
            let a = r1.recv().await.unwrap();
            let b = r2.recv().await.unwrap();
            assert!(
                (a == b"from s1" && b == b"from s2") || (a == b"from s2" && b == b"from s1"),
                "frames crossed connections: {a:?} / {b:?}"
            );
        });
    }
}
