//! The I/O driver: each thread has one, and it wakes tasks when their descriptors are ready
//! or their timers are due.

mod async_fd;
mod timer;

pub use async_fd::AsyncFd;
pub use timer::Timer;

use polling::{Event, Events, PollMode, Poller};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::os::fd::{AsFd, BorrowedFd, RawFd};
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Waker};
use std::thread::ThreadId;
use std::time::Instant;

thread_local! {
    /// Each thread creates its own driver on first use.
    static DRIVER: Driver = Driver::new();
}

/// Watches this thread's descriptors and wakes tasks when I/O can be retried.
pub(crate) struct Driver {
    poller: Arc<Poller>, // Queries the OS for I/O readiness.
    thread: ThreadId,    // Identifies the thread that owns this driver.
    entries: RefCell<HashMap<usize, Rc<RegEntry>>>, // Maps poller keys to descriptor registrations.
    events: RefCell<Events>, // Reuses the event buffer across waits.
    timers: RefCell<BTreeMap<(Instant, usize), Waker>>, // Orders the pending timers by due time.
    next_timer_id: Cell<usize>,
}

/// Allows another thread to interrupt the driver's blocking wait.
#[derive(Clone)]
pub(crate) struct Notifier {
    poller: Arc<Poller>,
    thread: ThreadId, // Identifies the driver thread.
}

impl Notifier {
    /// Interrupt the driver only for cross-thread wakes; local wakes are already queued.
    pub(crate) fn notify(&self) {
        if std::thread::current().id() != self.thread {
            self.poller.notify().expect("cannot notify the I/O poller");
        }
    }
}

/// Tracks read and write waiters for a descriptor owned by `Registered<T>`.
struct RegEntry {
    fd: RawFd,                      // Borrowed until the owner deregisters and closes it.
    reader: RefCell<Option<Waker>>, // One read waiter shared by all descriptor clones.
    writer: RefCell<Option<Waker>>, // One write waiter shared by all descriptor clones.
}

impl RegEntry {
    /// Replace the read waiter and request a wake when reading can be retried.
    fn wait_readable(&self, cx: &Context<'_>) -> io::Result<()> {
        self.wait(&self.reader, cx)
    }

    /// Replace the write waiter and request a wake when writing can be retried.
    fn wait_writable(&self, cx: &Context<'_>) -> io::Result<()> {
        self.wait(&self.writer, cx)
    }

    /// Store the task's waker before enabling its next readiness notification.
    fn wait(&self, waiter: &RefCell<Option<Waker>>, cx: &Context<'_>) -> io::Result<()> {
        *waiter.borrow_mut() = Some(cx.waker().clone());
        Driver::with(|driver| driver.arm(self))
    }

    /// Use the descriptor number as the poller key.
    fn key(&self) -> usize {
        self.fd as usize
    }

    /// Wake and remove each waiter whose I/O direction is ready.
    fn wake(&self, event: &Event) {
        for (ready, waiter) in [
            (event.readable, &self.reader),
            (event.writable, &self.writer),
        ] {
            if ready && let Some(waker) = waiter.borrow_mut().take() {
                waker.wake();
            }
        }
    }
}

impl AsFd for RegEntry {
    /// Borrow the registered descriptor for a poller operation.
    fn as_fd(&self) -> BorrowedFd<'_> {
        // SAFETY: The descriptor owner deregisters this entry before closing the descriptor.
        unsafe { BorrowedFd::borrow_raw(self.fd) }
    }
}

impl Driver {
    fn new() -> Self {
        Self {
            poller: Arc::new(Poller::new().expect("cannot create the I/O poller")),
            thread: std::thread::current().id(),
            entries: RefCell::new(HashMap::new()),
            events: RefCell::new(Events::new()),
            timers: RefCell::new(BTreeMap::new()),
            next_timer_id: Cell::new(0),
        }
    }

    /// Access the driver owned by the current thread.
    pub(crate) fn with<R>(f: impl FnOnce(&Driver) -> R) -> R {
        DRIVER.with(f)
    }

    /// Create a handle that can wake this thread's driver from another thread.
    pub(crate) fn notifier() -> Notifier {
        Self::with(|driver| Notifier {
            poller: driver.poller.clone(),
            thread: driver.thread,
        })
    }

    fn next_timer_id(&self) -> usize {
        self.next_timer_id.replace(self.next_timer_id.get() + 1)
    }

    /// Register a descriptor with no initial read or write interest.
    fn register(&self, fd: RawFd) -> io::Result<Rc<RegEntry>> {
        let entry = Rc::new(RegEntry {
            fd,
            reader: RefCell::new(None),
            writer: RefCell::new(None),
        });
        // SAFETY: The descriptor owner deregisters this entry before closing the descriptor.
        unsafe {
            self.poller
                .add_with_mode(fd, Event::none(entry.key()), PollMode::Oneshot)?
        };
        self.entries.borrow_mut().insert(entry.key(), entry.clone());
        Ok(entry)
    }

    /// Remove a descriptor from the poller and the registration map.
    fn deregister(&self, entry: &RegEntry) {
        self.entries.borrow_mut().remove(&entry.key());
        let _ = self.poller.delete(entry);
    }

    /// Enable readiness notifications for directions with a registered waiter.
    fn arm(&self, entry: &RegEntry) -> io::Result<()> {
        let readable = entry.reader.borrow().is_some();
        let writable = entry.writer.borrow().is_some();
        if readable || writable {
            self.poller
                .modify(entry, Event::new(entry.key(), readable, writable))?;
        }
        Ok(())
    }

    /// Wait for I/O, an alarm, a timer, or a notification, then wake tasks with ready I/O
    /// and the timers that are due.
    pub(crate) fn wait(&self, alarm_due: Option<Instant>) -> io::Result<()> {
        let mut events = self.events.borrow_mut();
        events.clear();
        let timer_due = self.timers.borrow().keys().next().map(|(due, _)| *due);
        match [alarm_due, timer_due].into_iter().flatten().min() {
            Some(due) => self.poller.wait_deadline(&mut events, due)?,
            None => self.poller.wait(&mut events, None)?,
        };
        let entries = self.entries.borrow();
        for event in events.iter() {
            let Some(entry) = entries.get(&event.key) else {
                continue; // Ignore events for descriptors that are no longer registered.
            };
            entry.wake(&event);
            self.arm(entry)?;
        }
        let (mut timers, now) = (self.timers.borrow_mut(), Instant::now());
        while let Some(timer) = timers.first_entry().filter(|timer| timer.key().0 <= now) {
            timer.remove().wake();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::async_fd::tests::pair;
    use super::*;
    use crate::runtime::Runtime;
    use futures::io::{AsyncReadExt, AsyncWriteExt};
    use std::time::Duration;

    #[test]
    fn the_wait_ends_when_the_alarm_is_due_without_io() {
        let runtime = Runtime::new();
        let (_client, _server) = pair(); // No I/O should interrupt the wait.
        let start = Instant::now();
        let alarm_due = start + Duration::from_millis(20);
        assert!(runtime.wait_wake_until(Some(alarm_due)).is_none());
        assert!(Instant::now() >= alarm_due);
    }

    #[test]
    fn an_io_wake_does_not_cut_the_next_wait_short() {
        let runtime = Runtime::new();
        let (mut client, mut server) = pair();
        runtime.block_on(async move {
            let mut buf = [0u8; 5];
            let (n, _) = futures::join!(server.read(&mut buf), client.write_all(b"hello"));
            assert_eq!(n.unwrap(), 5);
        });
        let start = Instant::now();
        let alarm_due = start + Duration::from_millis(20);
        assert!(runtime.wait_wake_until(Some(alarm_due)).is_none());
        assert!(Instant::now() >= alarm_due);
    }

    #[test]
    fn a_wake_from_another_thread_interrupts_the_wait() {
        let runtime = Runtime::new();
        let (_client, _server) = pair();
        let task = runtime.spawn(async {});
        runtime.next_wake(); // Discard the wake that schedules the first poll.
        let waker = runtime.waker_for(task, crate::runtime::WakeKind::Physical);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            waker.wake();
        });
        let start = Instant::now();
        let wake = runtime.wait_wake_until(Some(start + Duration::from_secs(5)));
        assert_eq!(wake.map(|(id, _)| id), Some(task));
        assert!(start.elapsed() < Duration::from_secs(1));
    }
}
