use std::time::Instant;

use super::NodeScheduler;
use super::shutdown::Stage;
use crate::node::ExecutionMode;
use crate::time::Time;

impl NodeScheduler {
    /// Advance logical time and deliver all events through the new timestamp.
    pub fn advance_to(&self, time: Time) {
        assert!(
            time > self.now.get(),
            "cannot advance to {time} from {}",
            self.now.get()
        );
        self.now.set(time);
        self.timestamp_complete.set(false);
        let mut physical = false;
        for source in self.event_sources.borrow().iter() {
            physical |= source.wake_wakers(time);
        }
        self.physical.set(physical);
    }

    /// Send the current timestamp's output and ENT promise exactly once.
    pub(super) fn send_frame(&self, ent: impl FnOnce() -> Time) {
        if self.timestamp_complete.get() {
            return;
        }
        let (timestamp, ent) = (self.now(), ent());
        assert!(
            ent > timestamp,
            "ENT would be less than timestamp: {ent} < {timestamp}"
        );
        for sink in self.event_sinks.borrow().iter() {
            sink.send(timestamp, ent, self.physical.get());
        }
        self.timestamp_complete.set(true);
    }

    /// Return the minimum ENT across all sources, excluding physical events.
    pub fn ent(&self) -> Time {
        self.event_sources
            .borrow()
            .iter()
            .map(|source| source.ent())
            .min()
            .unwrap_or(Time::FOREVER)
    }

    /// Return the earliest event timestamp that every source's ordering constraints permit.
    pub(super) fn next_safe_timestamp(&self) -> Option<Time> {
        let sources = self.event_sources.borrow();
        let ready: Vec<Option<Time>> = sources
            .iter()
            .map(|source| source.next_event_timestamp())
            .collect();
        // Each reported timestamp rules out earlier arrivals from that source.
        let queued = ready.iter().flatten().min().copied();
        let next = self.shutdown.get().next_timestamp(queued)?;
        // STAT (safe to advance to): the earliest ENT among the sources that report no timestamp.
        let stat = sources
            .iter()
            .zip(&ready)
            .filter(|(_, ready)| ready.is_none())
            .map(|(source, _)| source.ent())
            .min()
            .unwrap_or(Time::FOREVER);
        // Equality must wait: a source with ENT = 2s may still deliver at 2s.
        (next > self.now() && next < stat).then_some(next)
    }

    /// Return when a timestamp is due on the wall clock, or `None` in FastForward mode.
    pub(super) fn due(&self, timestamp: Time) -> Option<Instant> {
        match self.execution_mode.get() {
            ExecutionMode::Realtime => Some(self.start_time.get() + timestamp.duration()),
            ExecutionMode::FastForward => None,
        }
    }

    /// Advance if the next timestamp is due; return whether time advanced.
    pub(super) fn advance_if_due(&self, next: Option<Time>) -> bool {
        match next {
            // A timestamp with no due time is not paced by the wall clock.
            Some(timestamp) if self.due(timestamp).is_none_or(|due| due <= Instant::now()) => {
                if self.shutdown.get() == Stage::At(timestamp) {
                    self.finish_at(timestamp, false);
                } else {
                    self.advance_to(timestamp);
                }
                true
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::time::Duration;

    use super::super::testing::{FixedSource, RecordingSink};
    use super::*;
    use crate::app::Builder;
    use crate::node::alarms::Alarms;
    use crate::runtime::Runtime;
    use crate::time::{MS, S};

    #[test]
    fn ent_is_the_min_over_all_event_sources() {
        let scheduler = NodeScheduler::fast_forward();
        assert_eq!(scheduler.ent(), Time::FOREVER);
        scheduler.add_source(FixedSource::new(5 * S, true));
        // An empty source's ENT still constrains the node.
        scheduler.add_source(FixedSource::new(2 * S, false));
        assert_eq!(scheduler.ent(), 2 * S);
    }

    #[test]
    fn try_advance_delivers_the_earliest_consumable() {
        let scheduler = NodeScheduler::fast_forward();
        // One source already has a value at 1s.
        scheduler.add_source(FixedSource::new(1 * S, true));
        // Another source rules out values before 2s.
        scheduler.add_source(FixedSource::new(2 * S, false));

        assert!(scheduler.try_advance());
        assert_eq!(scheduler.now(), 1 * S); // The 1s event is earlier than every unresolved ENT.
    }

    #[test]
    fn try_advance_blocks_on_an_equal_possibility() {
        let scheduler = NodeScheduler::fast_forward();
        // The left source already has its 2s event.
        scheduler.add_source(FixedSource::new(2 * S, true));
        let announced = FixedSource::new(2 * S, false); // The right source may still deliver at 2s.
        scheduler.add_source(announced.clone());

        assert!(!scheduler.try_advance());
        // The right source has not resolved its 2s event yet.
        assert_eq!(scheduler.now(), Time::ZERO);

        announced.ready.set(Some(2 * S)); // The right source's simultaneous event has arrived.
        assert!(scheduler.try_advance());
        assert_eq!(scheduler.now(), 2 * S); // Both sources can now deliver at the same timestamp.
    }

    #[test]
    fn closed_upstream_does_not_veto() {
        let scheduler = NodeScheduler::fast_forward();
        // This source has closed and has no queued values.
        scheduler.add_source(FixedSource::new(Time::FOREVER, false));
        scheduler.add_source(FixedSource::new(3 * S, true)); // Another source has a value at 3s.

        scheduler.try_advance();
        assert_eq!(scheduler.now(), 3 * S); // The closed source cannot block the 3s event.
    }

    #[test]
    fn a_timestamp_is_completed_once() {
        let scheduler = NodeScheduler::fast_forward();
        let sink = RecordingSink::new();
        scheduler.add_sink(sink.clone());
        let source = FixedSource::new(1 * S, true);
        scheduler.add_source(source.clone());

        scheduler.try_advance();
        assert!(sink.sent.borrow().is_empty()); // A timestamp's frame is sent after its tasks run.
        assert_eq!(scheduler.now(), 1 * S);

        source.ent.set(Time::FOREVER); // The source has no remaining logical events.
        source.ready.set(None);
        scheduler.try_advance();
        assert_eq!(*sink.sent.borrow(), vec![(1 * S, Time::FOREVER, false)]);

        scheduler.try_advance();
        // A completed timestamp never sends a second frame.
        assert_eq!(sink.sent.borrow().len(), 1);
    }

    #[test]
    fn an_elapsed_wait_is_delivered_before_later_inputs() {
        let scheduler = NodeScheduler::fast_forward();
        let alarms = Rc::new(Alarms::new());
        let alarm = alarms.set(700 * MS, false);
        scheduler.add_source(alarms.clone());
        scheduler.add_source(FixedSource::new(1 * S, true));
        scheduler.add_source(FixedSource::new(2 * S, false));

        scheduler.try_advance();
        assert_eq!(scheduler.now(), 700 * MS);
        assert!(!scheduler.is_physical()); // This alarm was created at a logical timestamp.

        drop(alarm); // Completing the wait removes its alarm.
        scheduler.try_advance();
        assert_eq!(scheduler.now(), 1 * S);
    }

    #[test]
    #[should_panic(expected = "ENT would be less than timestamp")]
    fn an_unconsumed_event_at_the_idle_point_is_a_scheduling_bug() {
        let scheduler = NodeScheduler::fast_forward();
        let alarms = Rc::new(Alarms::new());
        let _unconsumed = alarms.set(700 * MS, false);
        scheduler.add_source(alarms.clone());

        scheduler.try_advance();
        assert_eq!(scheduler.now(), 700 * MS);
        // An unconsumed alarm would make ENT equal the completed timestamp, violating ENT > now.
        scheduler.try_advance();
    }

    #[test]
    fn concurrent_waits_deliver_in_timestamp_order() {
        let scheduler = NodeScheduler::fast_forward();
        let alarms = Rc::new(Alarms::new());
        let first = alarms.set(700 * MS, false);
        let second = alarms.set(1 * S, false);
        scheduler.add_source(alarms.clone());
        scheduler.add_source(FixedSource::new(2 * S, false));

        scheduler.try_advance();
        assert_eq!(scheduler.now(), 700 * MS);

        drop(first);
        scheduler.try_advance();
        assert_eq!(scheduler.now(), 1 * S);

        drop(second);
        scheduler.try_advance();
        assert_eq!(scheduler.now(), 1 * S); // No remaining source has an event to deliver.
    }

    #[test]
    fn try_advance_without_a_ready_head_is_a_no_op() {
        let scheduler = NodeScheduler::fast_forward();
        assert!(!scheduler.try_advance());
        assert_eq!(scheduler.now(), Time::ZERO); // No source has a pending event.

        // This source has an ENT promise but no event.
        scheduler.add_source(FixedSource::new(5 * S, false));
        assert!(!scheduler.try_advance());
        assert_eq!(scheduler.now(), Time::ZERO);
    }

    #[test]
    fn a_physical_source_is_delivered_but_never_promised() {
        let scheduler = NodeScheduler::fast_forward();
        let sink = RecordingSink::new();
        scheduler.add_sink(sink.clone());
        let action = FixedSource::physical(3 * S);
        scheduler.add_source(action.clone());
        // An empty input promises no logical values before 5s.
        scheduler.add_source(FixedSource::new(5 * S, false));
        assert_eq!(scheduler.ent(), 5 * S); // Physical events do not lower the node's ENT.

        scheduler.try_advance();
        assert_eq!(scheduler.now(), 3 * S); // Physical events still advance logical time.

        action.ready.set(None); // The physical event has been consumed.
        scheduler.try_advance();
        // The output frame records the timestamp's physical origin.
        assert_eq!(*sink.sent.borrow(), vec![(3 * S, 5 * S, true)]);
    }

    #[test]
    #[should_panic(expected = "already completed")]
    fn setting_an_output_at_the_start_timestamp_is_a_bug() {
        let scheduler = NodeScheduler::fast_forward();
        let mut builder = Builder::new(0, scheduler);
        let out = builder.output::<i64>();
        out.set(1); // The initial timestamp is already complete, so setting output must fail.
    }

    #[test]
    fn a_pending_physical_wait_is_delivered_at_its_own_timestamp() {
        let scheduler = NodeScheduler::fast_forward();
        let _alarm = scheduler.alarms.set(2 * S, true);
        scheduler.add_source(FixedSource::new(5 * S, true));
        assert_eq!(scheduler.ent(), 5 * S); // Physical alarms are excluded from ENT.

        scheduler.try_advance();
        // The physical alarm executes before the later logical event.
        assert_eq!(scheduler.now(), 2 * S);
    }

    #[test]
    fn the_default_mode_lets_physical_time_reach_the_timestamp() {
        let scheduler = NodeScheduler::new(Rc::new(Runtime::new()));
        let _alarm = scheduler.alarms.set(2 * S, false);

        assert!(!scheduler.try_advance());
        // The timestamp is not yet due on the wall clock.
        assert_eq!(scheduler.now(), Time::ZERO);

        scheduler
            .start_time
            .set(Instant::now() - Duration::from_secs(2));
        assert!(scheduler.try_advance());
        assert_eq!(scheduler.now(), 2 * S);
    }

    #[test]
    fn the_clock_starts_at_zero_and_jumps() {
        let scheduler = NodeScheduler::fast_forward();
        assert_eq!(scheduler.now(), Time::ZERO);
        scheduler.advance_to(Time::new(10));
        assert_eq!(scheduler.now(), Time::new(10));
    }

    #[test]
    #[should_panic(expected = "cannot advance")]
    fn the_clock_is_monotonic() {
        let scheduler = NodeScheduler::fast_forward();
        scheduler.advance_to(Time::new(10));
        scheduler.advance_to(Time::new(3));
    }

    #[test]
    #[should_panic(expected = "cannot advance")]
    fn the_clock_rejects_the_current_timestamp() {
        let scheduler = NodeScheduler::fast_forward();
        scheduler.advance_to(Time::new(10));
        scheduler.advance_to(Time::new(10)); // Advancing to the current timestamp must fail.
    }
}
