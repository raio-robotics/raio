use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::task::Waker;

use super::EventSource;
use crate::time::Time;

/// Pending waits that wake when the node reaches their target tags.
pub(super) struct Alarms {
    entries: RefCell<Vec<Weak<Alarm>>>, // Weak references let dropping an alarm cancel its wait.
}

/// A pending wait, cancelled when its owning future drops the alarm.
pub(super) struct Alarm {
    target: Time,
    physical: bool, // The wait inherits the tag's physical origin.
    pub(super) waker: RefCell<Option<Waker>>,
}

impl Alarms {
    pub(super) fn new() -> Self {
        Self {
            entries: RefCell::new(Vec::new()),
        }
    }

    /// Create an alarm for `target`; keep the returned handle alive until the wait ends.
    pub(super) fn set(&self, target: Time, physical: bool) -> Rc<Alarm> {
        let alarm = Rc::new(Alarm {
            target,
            physical,
            waker: RefCell::new(None),
        });
        let mut entries = self.entries.borrow_mut();
        entries.retain(|entry| entry.strong_count() > 0);
        entries.push(Rc::downgrade(&alarm));
        alarm
    }

    /// Return the earliest target among active alarms matching `filter`.
    fn earliest(&self, filter: impl Fn(&Alarm) -> bool) -> Option<Time> {
        self.entries
            .borrow()
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|alarm| filter(alarm))
            .map(|alarm| alarm.target)
            .min()
    }
}

impl EventSource for Alarms {
    fn ent(&self) -> Time {
        self.earliest(|alarm| !alarm.physical)
            .unwrap_or(Time::FOREVER)
    }

    fn next_event_tag(&self) -> Option<Time> {
        self.earliest(|_| true)
    }

    fn wake_wakers(&self, time: Time) -> bool {
        let mut physical = false;
        for alarm in self.entries.borrow().iter().filter_map(Weak::upgrade) {
            if alarm.target <= time {
                physical |= alarm.physical;
                if let Some(waker) = alarm.waker.borrow_mut().take() {
                    waker.wake();
                }
            }
        }
        physical
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::{MS, S};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Wake;

    struct Flag(AtomicBool);
    impl Wake for Flag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn an_alarm_wakes_its_task_at_its_own_tag() {
        let alarms = Alarms::new();
        let alarm = alarms.set(1 * S, false);
        let flag = Arc::new(Flag(AtomicBool::new(false)));
        *alarm.waker.borrow_mut() = Some(Waker::from(flag.clone()));

        alarms.wake_wakers(500 * MS);
        assert!(!flag.0.load(Ordering::SeqCst)); // The target tag has not been reached.

        alarms.wake_wakers(1 * S);
        assert!(flag.0.load(Ordering::SeqCst)); // Reaching the target tag wakes the waiting task.
    }

    #[test]
    fn an_alarm_set_at_a_physical_tag_is_physical() {
        let alarms = Alarms::new();
        let _physical = alarms.set(1 * S, true);
        assert_eq!(alarms.ent(), Time::FOREVER); // Physical alarms do not limit the logical ENT.
        // Physical alarms still participate in event ordering.
        assert_eq!(alarms.next_event_tag(), Some(1 * S));
        assert!(alarms.wake_wakers(1 * S)); // Delivering the alarm marks the tag physical.

        let _logical = alarms.set(2 * S, false);
        assert_eq!(alarms.ent(), 2 * S);
    }

    #[test]
    fn a_dropped_alarm_is_cancelled() {
        let alarms = Alarms::new();
        let alarm = alarms.set(1 * S, false);
        assert_eq!(alarms.next_event_tag(), Some(1 * S));
        drop(alarm);
        assert_eq!(alarms.next_event_tag(), None);
    }
}
