pub mod tcp;

use std::future::Future;

pub trait Transport {
    type Sender: Sender;
    type Receiver: Receiver;

    /// Start listening at a new local address.
    fn bind() -> impl Future<Output = Self>;

    fn endpoint(&self) -> &str;

    /// Accept one inbound connection.
    fn accept(&mut self) -> impl Future<Output = (Self::Sender, Self::Receiver)>;

    /// Connect to a remote endpoint.
    fn connect(endpoint: &str) -> impl Future<Output = (Self::Sender, Self::Receiver)>;
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
