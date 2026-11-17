use super::{Driver, RegEntry};
use std::io::{self, Read, Write};
use std::os::fd::{AsFd, AsRawFd};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

/// A thread-local async descriptor with one reader and one writer shared across clones.
pub struct AsyncFd<T: AsFd> {
    inner: Rc<Registered<T>>, // The last clone owns descriptor cleanup.
}

/// Owns the descriptor and deregisters it before the descriptor closes.
struct Registered<T: AsFd> {
    io: T,               // Owns the open descriptor.
    entry: Rc<RegEntry>, // Shares the descriptor's registration with the driver.
}

impl<T: AsFd> Drop for Registered<T> {
    /// Deregister from the driver before `io` closes.
    fn drop(&mut self) {
        Driver::with(|driver| driver.deregister(&self.entry));
    }
}

impl<T: AsFd> Clone for AsyncFd<T> {
    /// Share the descriptor and registration without duplicating the OS descriptor.
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: AsFd> AsyncFd<T> {
    /// Register `io` on this thread; the caller must first enable nonblocking mode.
    pub fn new(io: T) -> io::Result<Self> {
        let entry = Driver::with(|driver| driver.register(io.as_fd().as_raw_fd()))?;
        Ok(Self {
            inner: Rc::new(Registered { io, entry }),
        })
    }

    /// Borrow the underlying I/O object; keep its descriptor open and nonblocking.
    pub fn get_ref(&self) -> &T {
        &self.inner.io
    }

    /// Try `op`; wait for readability and retry whenever it returns `WouldBlock`.
    pub async fn read_with<R>(&self, mut op: impl FnMut(&T) -> io::Result<R>) -> io::Result<R> {
        std::future::poll_fn(|cx| self.poll_with(cx, RegEntry::wait_readable, &mut op)).await
    }

    /// Try `op`; wait for writability and retry whenever it returns `WouldBlock`.
    pub async fn write_with<R>(&self, mut op: impl FnMut(&T) -> io::Result<R>) -> io::Result<R> {
        std::future::poll_fn(|cx| self.poll_with(cx, RegEntry::wait_writable, &mut op)).await
    }

    /// Try the operation first; register a wake only if it returns `WouldBlock`.
    fn poll_with<R>(
        &self,
        cx: &mut Context<'_>,
        wait: fn(&RegEntry, &Context<'_>) -> io::Result<()>,
        op: &mut impl FnMut(&T) -> io::Result<R>,
    ) -> Poll<io::Result<R>> {
        match op(self.get_ref()) {
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                match wait(&self.inner.entry, cx) {
                    Ok(()) => Poll::Pending,
                    Err(err) => Poll::Ready(Err(err)),
                }
            }
            result => Poll::Ready(result),
        }
    }
}

impl<T: AsFd> futures::io::AsyncRead for AsyncFd<T>
where
    for<'a> &'a T: Read,
{
    /// Try reading into `buf`; suspend and request a wake if reading would block.
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_with(cx, RegEntry::wait_readable, &mut |mut io| io.read(buf))
    }
}

impl<T: AsFd> futures::io::AsyncWrite for AsyncFd<T>
where
    for<'a> &'a T: Write,
{
    /// Try writing from `buf`; suspend and request a wake if writing would block.
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_with(cx, RegEntry::wait_writable, &mut |mut io| io.write(buf))
    }

    /// Try flushing; suspend and request a wake if flushing would block.
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_with(cx, RegEntry::wait_writable, &mut |mut io| io.flush())
    }

    /// Flush pending writes; dropping the last clone closes the descriptor.
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::runtime::Runtime;
    use futures::io::{AsyncReadExt, AsyncWriteExt};
    use std::net::{TcpListener, TcpStream};

    /// Create two connected, nonblocking TCP sockets registered with this thread's driver.
    pub(crate) fn pair() -> (AsyncFd<TcpStream>, AsyncFd<TcpStream>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        client.set_nonblocking(true).unwrap();
        server.set_nonblocking(true).unwrap();
        (AsyncFd::new(client).unwrap(), AsyncFd::new(server).unwrap())
    }

    #[test]
    fn a_read_waits_for_data_and_a_write_delivers_it() {
        Runtime::new().block_on(async {
            let (mut client, mut server) = pair();
            let mut buf = [0u8; 5];
            let (_, n) = futures::join!(client.write_all(b"hello"), server.read(&mut buf));
            assert_eq!(n.unwrap(), 5);
            assert_eq!(&buf, b"hello");
        });
    }

    #[test]
    fn the_last_clone_deregisters() {
        Runtime::new().block_on(async {
            let (client, _server) = pair();
            let key = client.inner.entry.key();
            let registered = || Driver::with(|driver| driver.entries.borrow().contains_key(&key));
            let other = client.clone();
            drop(client);
            assert!(registered());
            drop(other);
            assert!(!registered());
        });
    }
}
