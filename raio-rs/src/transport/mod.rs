pub mod tcp;

use std::future::Future;
use std::rc::Rc;

use futures::StreamExt;
use futures::channel::mpsc::UnboundedReceiver;

use crate::runtime::BoxFuture;

/// Creates the listeners and connections of one kind of link.
pub trait Transport {
    type Listener: Listener<Sender = Self::Sender, Receiver = Self::Receiver>;
    type Sender: Sender;
    type Receiver: Receiver;

    /// The name by which a connection selects this transport.
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }

    /// Start listening at a new local address.
    fn bind(&self) -> impl Future<Output = Self::Listener>;

    /// Connect to a remote endpoint.
    fn connect(&self, endpoint: &str) -> impl Future<Output = (Self::Sender, Self::Receiver)>;
}

pub trait Listener {
    type Sender: Sender;
    type Receiver: Receiver;

    fn endpoint(&self) -> &str;

    /// Accept one inbound connection.
    fn accept(&mut self) -> impl Future<Output = (Self::Sender, Self::Receiver)>;
}

pub trait Sender {
    /// Send a message to the remote endpoint.
    fn send(&mut self, msg: &[u8]) -> impl Future<Output = ()>;

    /// Send a request and wait for the reply on the same connection.
    fn call(&mut self, request: &[u8]) -> impl Future<Output = Vec<u8>>;
}

pub trait Receiver {
    /// Receive one complete message, or `None` when the peer closes the connection.
    fn recv(&mut self) -> impl Future<Output = Option<Vec<u8>>>;
}

/// Accepts one connection and passes each frame it receives to the callback.
pub(crate) type Accept = Box<dyn FnOnce(Box<dyn FnMut(Vec<u8>)>) -> BoxFuture<()>>;

/// A transport whose connection types are hidden from the ports that use it.
pub(crate) trait AnyTransport {
    fn name(&self) -> &str;
    /// Start listening and return the address and the task builder for its one connection.
    fn listen(self: Rc<Self>) -> BoxFuture<(String, Accept)>;
    /// Connect to an endpoint and send it every queued frame.
    fn send(self: Rc<Self>, endpoint: String, frames: UnboundedReceiver<Vec<u8>>) -> BoxFuture<()>;
}

impl<Tr: Transport + 'static> AnyTransport for Tr {
    fn name(&self) -> &str {
        Transport::name(self)
    }

    fn listen(self: Rc<Self>) -> BoxFuture<(String, Accept)> {
        Box::pin(async move {
            let mut listener = self.bind().await;
            let endpoint = listener.endpoint().to_string();
            let accept: Accept = Box::new(move |mut on_frame| {
                Box::pin(async move {
                    let (_sender, mut receiver) = listener.accept().await;
                    while let Some(frame) = receiver.recv().await {
                        on_frame(frame);
                    }
                })
            });
            (endpoint, accept)
        })
    }

    fn send(
        self: Rc<Self>,
        endpoint: String,
        mut frames: UnboundedReceiver<Vec<u8>>,
    ) -> BoxFuture<()> {
        Box::pin(async move {
            let (mut sender, _receiver) = self.connect(&endpoint).await;
            while let Some(frame) = frames.next().await {
                sender.send(&frame).await;
            }
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::tcp::{TcpListener, TcpReceiver, TcpSender, TcpTransport};
    use super::*;
    use crate::runtime::Runtime;
    use futures::channel::mpsc::unbounded;
    use std::cell::RefCell;

    /// A second transport: TCP under its type name, with addresses that only it can connect to.
    pub(crate) struct Loopback;

    pub(crate) struct LoopbackListener {
        tcp: TcpListener,
        endpoint: String,
    }

    impl Transport for Loopback {
        type Listener = LoopbackListener;
        type Sender = TcpSender;
        type Receiver = TcpReceiver;

        async fn bind(&self) -> LoopbackListener {
            let tcp = TcpTransport.bind().await;
            let endpoint = format!("loopback://{}", tcp.endpoint());
            LoopbackListener { tcp, endpoint }
        }

        async fn connect(&self, endpoint: &str) -> (TcpSender, TcpReceiver) {
            let endpoint = endpoint.strip_prefix("loopback://").unwrap();
            TcpTransport.connect(endpoint).await
        }
    }

    impl Listener for LoopbackListener {
        type Sender = TcpSender;
        type Receiver = TcpReceiver;

        fn endpoint(&self) -> &str {
            &self.endpoint
        }

        async fn accept(&mut self) -> (TcpSender, TcpReceiver) {
            self.tcp.accept().await
        }
    }

    #[test]
    fn a_transport_is_named_after_its_type_unless_it_says_otherwise() {
        assert_eq!(
            Transport::name(&Loopback),
            std::any::type_name::<Loopback>()
        );
        assert_eq!(Transport::name(&TcpTransport), "tcp");
    }

    #[test]
    fn a_transport_carries_frames_to_its_listener() {
        Runtime::new().block_on(async {
            let loopback: Rc<dyn AnyTransport> = Rc::new(Loopback);
            let (endpoint, accept) = loopback.clone().listen().await;
            let (frames, queued) = unbounded();
            frames.unbounded_send(b"one".to_vec()).unwrap();
            frames.unbounded_send(b"two".to_vec()).unwrap();
            drop(frames); // Closing the queue ends the sending task and closes the connection.
            let received = Rc::new(RefCell::new(Vec::new()));
            let seen = received.clone();
            futures::join!(
                loopback.send(endpoint, queued),
                accept(Box::new(move |frame| seen.borrow_mut().push(frame)))
            );
            assert_eq!(*received.borrow(), [b"one".to_vec(), b"two".to_vec()]);
        });
    }
}
