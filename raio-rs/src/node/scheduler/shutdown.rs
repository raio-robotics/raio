use super::NodeScheduler;
use crate::time::Time;

/// How far a node is into its app's shutdown.
#[derive(Clone, Copy, PartialEq, Debug)]
pub(super) enum Stage {
    /// No shutdown is under way.
    Running,
    /// The node offered this tag and stays before it until the coordinator names the shutdown tag.
    Held(Time),
    /// The node shuts down at this tag.
    At(Time),
    /// The node has shut down; only frame transfer still runs.
    Done,
}

impl Stage {
    /// Choose the node's next tag, given the earliest tag on its queue.
    pub(super) fn next_tag(self, queued: Option<Time>) -> Option<Time> {
        match self {
            Stage::Running => queued,
            Stage::Held(offer) => queued.filter(|tag| *tag < offer),
            // The shutdown tag follows every earlier event and replaces every later one.
            Stage::At(shutdown) => Some(queued.map_or(shutdown, |tag| tag.min(shutdown))),
            Stage::Done => None,
        }
    }
}

/// Carries a node's shutdown request and its delay to the coordinator.
pub(super) type Request = Box<dyn Fn(Time)>;

impl NodeScheduler {
    /// Ask the coordinator to shut down every node `delay` after the latest of their current tags.
    pub(crate) fn request_shutdown(&self, delay: Time) {
        if self.shutdown.get() != Stage::Running {
            return; // A shutdown is already under way.
        }
        // The node makes its own offer at once, so it does not run on past it.
        self.offer_shutdown(delay);
        let request = self.shutdown_request.borrow();
        request.as_ref().expect("the node has no coordinator")(delay);
    }

    /// Offer to shut down `delay` after the current tag, and stay before that tag until told.
    pub(crate) fn offer_shutdown(&self, delay: Time) -> Time {
        let offer = self.now() + delay;
        self.shutdown.set(Stage::Held(offer));
        offer
    }

    /// Shut down at this tag, which the coordinator chose for every node.
    pub(crate) fn shut_down_at(&self, tag: Time) {
        assert!(
            tag > self.now(),
            "cannot shut down at {tag} from {}",
            self.now()
        );
        self.shutdown.set(Stage::At(tag));
    }

    /// Move to the shutdown tag and shut down there; no task runs at that tag or after it.
    pub(super) fn stop(&self, tag: Time) {
        self.now.set(tag);
        self.tag_complete.set(false);
        self.physical.set(false);
        self.shut_down();
        self.shutdown.set(Stage::Done);
    }

    /// Run the shutdown function if it has not run, send any unsent final frame with
    /// ENT = FOREVER, then close the output sinks.
    pub(super) fn shut_down(&self) {
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
    use crate::node::{ExecutionMode, Node};
    use crate::runtime::Runtime;
    use crate::time::{MS, S};

    #[test]
    fn shutting_down_closes_each_drain() {
        let scheduler =
            NodeScheduler::with_execution_mode(Rc::new(Runtime::new()), ExecutionMode::FastForward);
        let sink = RecordingSink::new();
        scheduler.add_sink(sink.clone());
        scheduler.shut_down();
        assert!(sink.closed.get());
    }

    #[test]
    fn a_node_stays_before_the_tag_it_offered_until_the_shutdown_tag_is_named() {
        let scheduler =
            NodeScheduler::with_execution_mode(Rc::new(Runtime::new()), ExecutionMode::FastForward);
        let sink = RecordingSink::new();
        scheduler.add_sink(sink.clone());
        let source = FixedSource::new(1 * S, true);
        scheduler.add_source(source.clone());

        // The node offers its current tag plus the delay.
        assert_eq!(scheduler.offer_shutdown(2 * S), 2 * S);
        assert!(scheduler.try_advance());
        assert_eq!(scheduler.now(), 1 * S); // An event before the offered tag still runs.

        source.ent.set(3 * S);
        source.ready.set(Some(3 * S));
        assert!(!scheduler.try_advance());
        assert_eq!(scheduler.now(), 1 * S);

        scheduler.shut_down_at(4 * S);
        assert!(scheduler.try_advance());
        assert_eq!(scheduler.now(), 3 * S); // An event before the shutdown tag still runs.

        source.ent.set(Time::FOREVER);
        source.ready.set(None);
        assert!(scheduler.try_advance());
        assert_eq!(scheduler.now(), 4 * S);
        assert!(sink.closed.get());
        assert!(!scheduler.try_advance()); // A node that has shut down never advances again.
    }

    #[test]
    fn a_node_that_finishes_on_its_own_runs_its_shutdown_function_at_its_last_tag() {
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
    fn only_the_shutdown_function_runs_at_the_shutdown_tag() {
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
            let tag = node.wait(3 * MS).await;
            out.borrow_mut().push(tag);
            // This wait ends at the shutdown tag, where the task no longer runs.
            let tag = node.wait(2 * MS).await;
            out.borrow_mut().push(tag);
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
    fn a_node_in_a_shutdown_keeps_its_outputs_open_until_the_shutdown_tag() {
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

        let tag = scheduler.offer_shutdown(delay);
        assert_eq!(tag, (1 * MS).next_microstep());
        scheduler.shut_down_at(tag);
        scheduler.run_until_idle();
        assert_eq!(scheduler.now(), tag);
        assert!(sink.closed.get());
    }
}
