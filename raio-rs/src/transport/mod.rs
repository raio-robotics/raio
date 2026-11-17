pub mod tcp;

use std::future::Future;

pub trait Transport {
    type Sender: Sender;
    type Receiver: Receiver;

    fn new() -> impl Future<Output = Self> + Send;

    fn endpoint(&self) -> &str;

    /// Accept one inbound connection.
    fn accept(&mut self) -> impl Future<Output = Self::Receiver> + Send;

    /// Connect to a remote endpoint
    fn connect(&self, endpoint: &str) -> impl Future<Output = Self::Sender> + Send;
}

pub trait Sender {
    /// Send a message to the remote endpoint.
    fn send(&mut self, msg: &[u8]) -> impl Future<Output = ()> + Send;
}

pub trait Receiver {
    /// Receive a message on the local endpoint.
    fn recv(&mut self) -> impl Future<Output = Option<Vec<u8>>> + Send;
}
