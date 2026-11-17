use super::NodeScheduler;
use crate::time::Time;

/// The stage of its app's shutdown that a node is in.
#[derive(Clone, Copy, PartialEq, Debug)]
pub(super) enum Stage {
    /// No shutdown is under way.
    Running,
    /// The node offered this timestamp and stays before it until the coordinator names the
    /// timestamp of the shutdown.
    Held(Time),
    /// The node shuts down at this timestamp.
    At(Time),
    /// The app was interrupted: the node shuts down as soon as its current timestamp is complete.
    Now,
    /// The node has shut down; only frame transfer still runs.
    Done,
}

impl Stage {
    /// Choose the node's next timestamp, given the earliest timestamp on its queue.
    pub(super) fn next_timestamp(self, queued: Option<Time>) -> Option<Time> {
        match self {
            Stage::Running => queued,
            Stage::Held(offer) => queued.filter(|timestamp| *timestamp < offer),
            // The shutdown timestamp follows every earlier event and replaces every later one.
            Stage::At(shutdown) => {
                Some(queued.map_or(shutdown, |timestamp| timestamp.min(shutdown)))
            }
            Stage::Now | Stage::Done => None,
        }
    }
}

/// Carries a node's shutdown request and its delay to the coordinator.
pub(super) type Request = Box<dyn Fn(Time)>;

impl NodeScheduler {
    /// Ask the coordinator to shut down every node `delay` after the latest of their current
    /// timestamps.
    pub(crate) fn request_shutdown(&self, delay: Time) {
        if self.shutdown.get() != Stage::Running {
            return; // A shutdown is already under way.
        }
        // The node makes its own offer at once, so it does not run on past it.
        self.offer_shutdown(delay);
        let request = self.shutdown_request.borrow();
        request.as_ref().expect("the node has no coordinator")(delay);
    }

    /// Offer to shut down `delay` after the current timestamp, and stay before the offer until
    /// told.
    pub(crate) fn offer_shutdown(&self, delay: Time) -> Time {
        let offer = self.now() + delay;
        self.shutdown.set(Stage::Held(offer));
        offer
    }

    /// Shut down at this timestamp, which the coordinator chose for every node.
    pub(crate) fn shut_down_at(&self, timestamp: Time) {
        assert!(
            timestamp > self.now(),
            "cannot shut down at {timestamp} from {}",
            self.now()
        );
        self.shutdown.set(Stage::At(timestamp));
    }

    /// Shut down as soon as the current timestamp is complete, whatever the node has queued.
    pub(crate) fn shut_down_now(&self) {
        if self.shutdown.get() != Stage::Done {
            self.shutdown.set(Stage::Now);
        }
    }

    /// Move to the shutdown timestamp and finish there; no task runs at that timestamp or after it.
    pub(super) fn finish_at(&self, timestamp: Time, physical: bool) {
        self.now.set(timestamp);
        self.timestamp_complete.set(false);
        self.physical.set(physical);
        self.finish();
        self.shutdown.set(Stage::Done);
    }

    /// Run the shutdown function if it has not run, send any unsent final frame with
    /// ENT = FOREVER, then close the output sinks.
    pub(super) fn finish(&self) {
        let function = self.shutdown_function.borrow_mut().take();
        if let Some(function) = function {
            function();
        }
        self.send_frame(|| Time::FOREVER);
        for sink in self.event_sinks.borrow_mut().drain(..) {
            sink.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

    use super::super::testing::{FixedSource, RecordingSink, node_rig};
    use super::*;
    use crate::node::Node;
    use crate::time::{MS, S};

    #[test]
    fn finishing_closes_every_sink() {
        let scheduler = NodeScheduler::fast_forward();
        let sink = RecordingSink::new();
        scheduler.add_sink(sink.clone());
        scheduler.finish();
        assert!(sink.closed.get());
    }

    #[test]
    fn a_node_stays_before_the_timestamp_it_offered_until_the_shutdown_timestamp_is_named() {
        let scheduler = NodeScheduler::fast_forward();
        let sink = RecordingSink::new();
        scheduler.add_sink(sink.clone());
        let source = FixedSource::new(1 * S, true);
        scheduler.add_source(source.clone());

        // The node offers its current timestamp plus the delay.
        assert_eq!(scheduler.offer_shutdown(2 * S), 2 * S);
        assert!(scheduler.try_advance());
        assert_eq!(scheduler.now(), 1 * S); // An event before the offered timestamp still runs.

        source.ent.set(3 * S);
        source.ready.set(Some(3 * S));
        assert!(!scheduler.try_advance());
        assert_eq!(scheduler.now(), 1 * S);

        scheduler.shut_down_at(4 * S);
        assert!(scheduler.try_advance());
        assert_eq!(scheduler.now(), 3 * S); // An event before the shutdown timestamp still runs.

        source.ent.set(Time::FOREVER);
        source.ready.set(None);
        assert!(scheduler.try_advance());
        assert_eq!(scheduler.now(), 4 * S);
        assert!(sink.closed.get());
        assert!(!scheduler.try_advance()); // A node that has shut down never advances again.
    }

    #[test]
    fn a_node_that_finishes_on_its_own_runs_its_shutdown_function_at_its_last_timestamp() {
        let (_runtime, scheduler, sink) = node_rig();
        let node = Node {
            scheduler: scheduler.clone(),
        };
        let stopped = Rc::new(RefCell::new(Vec::new()));
        let (out, clock) = (stopped.clone(), scheduler.clone());
        *scheduler.shutdown_function.borrow_mut() =
            Some(Box::new(move || out.borrow_mut().push(clock.now())));
        scheduler.run_user(async move {
            node.wait(2 * MS).await;
        });
        assert_eq!(*stopped.borrow(), vec![2 * MS]);
        assert!(sink.closed.get());

        // A shutdown of the app that comes later does not run the function again.
        scheduler.shut_down_at(scheduler.offer_shutdown(1 * MS));
        scheduler.run_until_idle();
        assert_eq!(scheduler.now(), 3 * MS);
        assert_eq!(*stopped.borrow(), vec![2 * MS]);
    }

    #[test]
    fn only_the_shutdown_function_runs_at_the_shutdown_timestamp() {
        let (_runtime, scheduler, sink) = node_rig();
        let node = Node {
            scheduler: scheduler.clone(),
        };
        let woke = Rc::new(RefCell::new(Vec::new()));
        let stopped = Rc::new(Cell::new(None));
        let (out, clock) = (stopped.clone(), scheduler.clone());
        *scheduler.shutdown_function.borrow_mut() =
            Some(Box::new(move || out.set(Some(clock.now()))));
        scheduler.shut_down_at(5 * MS);

        let out = woke.clone();
        scheduler.run_user(async move {
            let timestamp = node.wait(3 * MS).await;
            out.borrow_mut().push(timestamp);
            // This wait ends at the shutdown timestamp, where the task no longer runs.
            let timestamp = node.wait(2 * MS).await;
            out.borrow_mut().push(timestamp);
        });
        assert_eq!(*woke.borrow(), vec![3 * MS]);
        assert_eq!(stopped.get(), Some(5 * MS));
        assert_eq!(
            *sink.sent.borrow(),
            vec![(3 * MS, 5 * MS, false), (5 * MS, Time::FOREVER, false)]
        );
        assert!(sink.closed.get());
    }

    #[test]
    fn an_interrupted_node_shuts_down_at_once_at_a_physical_timestamp() {
        let (_runtime, scheduler, sink) = node_rig();
        let node = Node {
            scheduler: scheduler.clone(),
        };
        let woke = Rc::new(Cell::new(false));
        let stopped = Rc::new(Cell::new(None));
        let (out, clock) = (stopped.clone(), scheduler.clone());
        *scheduler.shutdown_function.borrow_mut() = Some(Box::new(move || {
            out.set(Some((clock.now(), clock.is_physical())));
        }));
        let out = woke.clone();
        scheduler.spawn(async move {
            node.wait(1 * S).await;
            out.set(true);
        });

        scheduler.shut_down_now();
        scheduler.run_until_idle();
        // The node does not go on to the timestamp that its task waits for.
        let (timestamp, physical) = stopped.get().unwrap();
        assert!(timestamp > Time::ZERO && timestamp < 1 * S, "{timestamp}");
        assert!(physical);
        assert_eq!(*sink.sent.borrow(), vec![(timestamp, Time::FOREVER, true)]);
        assert!(sink.closed.get());

        scheduler.run_until_idle();
        assert!(!woke.get()); // No task runs after the shutdown.
        assert_eq!(scheduler.now(), timestamp);
    }

    #[test]
    fn a_node_in_a_shutdown_keeps_its_outputs_open_until_the_shutdown_timestamp() {
        let (_runtime, scheduler, sink) = node_rig();
        let requests = Rc::new(RefCell::new(Vec::new()));
        let sent = requests.clone();
        *scheduler.shutdown_request.borrow_mut() =
            Some(Box::new(move |delay| sent.borrow_mut().push(delay)));
        let node = Node {
            scheduler: scheduler.clone(),
        };
        scheduler.spawn(async move {
            node.wait(1 * MS).await;
            node.request_shutdown();
            node.request_shutdown_after(5 * MS); // One request is enough.
        });
        scheduler.run_until_idle();
        // A request that names no delay asks for one microstep.
        let delay = Time::ZERO.next_microstep();
        assert_eq!(*requests.borrow(), vec![delay]);
        // The node's only task has finished, yet its outputs stay open.
        assert!(!sink.closed.get());

        let timestamp = scheduler.offer_shutdown(delay);
        assert_eq!(timestamp, (1 * MS).next_microstep());
        scheduler.shut_down_at(timestamp);
        scheduler.run_until_idle();
        assert_eq!(scheduler.now(), timestamp);
        assert!(sink.closed.get());
    }
}
