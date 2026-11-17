use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::Poll;

/// Finishes with true when its input has a value, or with false once the input has closed.
pub type Trigger<'a> = Pin<Box<dyn Future<Output = bool> + 'a>>;

/// Wait for the next timestamp at which a trigger has a value; return false once all have closed.
pub async fn next_event(triggers: Vec<Trigger<'_>>) -> bool {
    let mut triggers: Vec<_> = triggers.into_iter().map(Some).collect();
    poll_fn(|cx| {
        let mut fired = false;
        // Every trigger is polled, so that one that closed cannot hide the value of another.
        for slot in &mut triggers {
            let Some(trigger) = slot else { continue };
            match trigger.as_mut().poll(cx) {
                Poll::Ready(true) => fired = true,
                Poll::Ready(false) => *slot = None,
                Poll::Pending => {}
            }
        }
        match fired || triggers.iter().all(Option::is_none) {
            true => Poll::Ready(fired),
            false => Poll::Pending,
        }
    })
    .await
}

/// Run the body once per timestamp at which any of the ports has a value, until all have closed.
#[macro_export]
macro_rules! reaction {
    ($self:ident, [$($port:ident),+ $(,)?], $body:block) => {{
        // A closed port stays closed, so every round may wait for all of the ports again.
        while $crate::app::next_event(vec![$($self.$port.trigger()),+]).await {
            $body
        }
    }};
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use crate::app::InputPort;
    use crate::node::NodeScheduler;
    use crate::time::{MS, NS, Time};

    struct Merge {
        left: InputPort<i64>,
        right: InputPort<i64>,
    }

    /// The timestamp of each run of a reaction, with the values that its inputs had then.
    type Runs = Rc<RefCell<Vec<(Time, Option<i64>, Option<i64>)>>>;

    /// Spawn a reaction to both inputs of a node on a FastForward scheduler.
    fn react() -> (Rc<NodeScheduler>, Rc<Merge>, Runs) {
        let scheduler = NodeScheduler::fast_forward();
        let (left, _) = InputPort::local(&scheduler);
        let (right, _) = InputPort::local(&scheduler);
        let merge = Rc::new(Merge { left, right });
        let runs = Runs::default();

        let (node, out, clock) = (merge.clone(), runs.clone(), scheduler.clone());
        scheduler.spawn(async move {
            reaction!(node, [left, right], {
                let (left, right) = (node.left.get(), node.right.get());
                let values = (left.map(|value| *value), right.map(|value| *value));
                out.borrow_mut().push((clock.now(), values.0, values.1));
            });
        });
        scheduler.run_until_idle();
        (scheduler, merge, runs)
    }

    #[test]
    fn a_reaction_runs_once_per_timestamp_at_which_any_port_has_a_value() {
        let (scheduler, merge, runs) = react();
        let (left, right) = (&merge.left.channel, &merge.right.channel);
        left.push(1 * MS, 1);
        left.push(2 * MS, 2);
        right.push(2 * MS, 20);
        right.push(3 * MS, 30);
        left.close();
        right.close();
        scheduler.run_until_idle();
        assert_eq!(
            *runs.borrow(),
            [
                (1 * MS, Some(1), None),
                (2 * MS, Some(2), Some(20)), // Both ports have a value: the body runs once.
                (3 * MS, None, Some(30)),
            ]
        );
    }

    #[test]
    fn a_port_that_closed_does_not_hide_the_value_of_another() {
        let (scheduler, merge, runs) = react();
        // The closed port and the value are found by the same poll of the reaction.
        merge.left.channel.close();
        merge.right.channel.push(NS, 7);
        merge.right.channel.update_remote_ent(1 * MS);
        scheduler.run_until_idle();
        assert_eq!(*runs.borrow(), [(NS, None, Some(7))]);
    }
}
