use super::Driver;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// Finishes once the wall clock reaches its due time; the driver of its thread wakes it.
pub struct Timer {
    due: Instant,
    /// Tells this timer from another one with the same due time.
    id: usize,
}

impl Timer {
    pub fn after(duration: Duration) -> Self {
        Self {
            due: Instant::now() + duration,
            id: Driver::with(Driver::next_timer_id),
        }
    }
}

impl Future for Timer {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if Instant::now() >= self.due {
            return Poll::Ready(());
        }
        let key = (self.due, self.id);
        Driver::with(|driver| driver.timers.borrow_mut().insert(key, cx.waker().clone()));
        Poll::Pending
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        Driver::with(|driver| driver.timers.borrow_mut().remove(&(self.due, self.id)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::Runtime;
    use futures::future::{Either, select};

    #[test]
    fn a_timer_finishes_once_its_time_has_passed() {
        let start = Instant::now();
        Runtime::new().block_on(Timer::after(Duration::from_millis(20)));
        assert!(start.elapsed() >= Duration::from_millis(20));
    }

    #[test]
    fn the_earlier_of_two_timers_finishes_first() {
        let start = Instant::now();
        let (late, early) = (Duration::from_secs(60), Duration::from_millis(10));
        Runtime::new().block_on(async move {
            let first = select(Timer::after(late), Timer::after(early)).await;
            assert!(matches!(first, Either::Right(_)));
        });
        // The late timer was dropped with the select, so the driver no longer waits for it.
        assert!(start.elapsed() < Duration::from_secs(1));
        Driver::with(|driver| assert!(driver.timers.borrow().is_empty()));
    }
}
