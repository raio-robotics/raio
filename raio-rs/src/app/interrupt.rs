use std::io::Read;
use std::os::fd::IntoRawFd;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicI32, Ordering};

/// The socket that the signal handler writes to once a launcher has installed it.
static WAKE: AtomicI32 = AtomicI32::new(-1);

extern "C" fn wake(_signal: libc::c_int) {
    // SAFETY: A signal handler may only call what is safe in one, and `write` is.
    unsafe { libc::write(WAKE.load(Ordering::Relaxed), [0u8].as_ptr().cast(), 1) };
}

/// Call `handler` on a thread of its own each time the user presses ^C.
pub(crate) fn on_interrupt(handler: impl Fn() + Send + 'static) {
    let (mut pressed, wake_end) = UnixStream::pair().expect("cannot create a socket pair");
    WAKE.store(wake_end.into_raw_fd(), Ordering::Relaxed);
    let wake = wake as extern "C" fn(libc::c_int);
    // SAFETY: The handler only writes to a socket that stays open while the program runs.
    unsafe { libc::signal(libc::SIGINT, wake as libc::sighandler_t) };
    std::thread::spawn(move || {
        while pressed.read_exact(&mut [0]).is_ok() {
            handler();
        }
    });
}

/// Leave ^C to the launcher, which tells its nodes to shut down through the coordinator.
pub(crate) fn ignore_interrupt() {
    // SAFETY: Ignoring a signal has no preconditions.
    unsafe { libc::signal(libc::SIGINT, libc::SIG_IGN) };
}
