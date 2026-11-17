use std::future::Future;

use super::NodeScheduler;
use super::shutdown::Stage;
use crate::runtime::{BoxFuture, TaskId, WakeKind};

impl NodeScheduler {
    /// Run until frame transfer and all user tasks finish, or until the node has shut down.
    pub(crate) fn run(
        &self,
        frame_transfer: impl Future<Output = ()> + 'static,
        tasks: Vec<BoxFuture<()>>,
    ) {
        let root = self.runtime.spawn_internal(frame_transfer);
        for task in tasks {
            self.spawn(task);
        }
        self.run_until(|| {
            // A node in a shutdown runs on until the shutdown timestamp.
            let finished = match self.shutdown.get() {
                Stage::Running => self.user_tasks_finished(),
                stage => stage == Stage::Done,
            };
            self.runtime.finished(root) && finished
        });
        // Dropping what still runs closes the node's link to the coordinator.
        self.runtime.clear();
    }

    /// Process task wakes and advance timestamps until idle and done.
    pub(super) fn run_until(&self, done: impl Fn() -> bool) {
        loop {
            if self.aborted.get() {
                return;
            }
            if let Some((task, kind)) = self.runtime.next_wake() {
                self.handle_wake(task, kind);
                continue;
            }

            // Every runnable task has run. A node whose tasks have all ended finishes here,
            // unless its app is shutting down: then it stays open until the shutdown timestamp.
            if self.user_tasks_finished() && self.shutdown.get() == Stage::Running {
                self.finish();
            }

            // Finish the current timestamp by sending its output and the node's ENT promise.
            self.send_frame(|| self.ent());

            if self.shutdown.get() == Stage::Now {
                // An interrupted node shuts down at a physical timestamp, ahead of all it has
                // queued.
                self.finish_at(self.mint_physical_timestamp(), true);
                continue;
            }

            let next = self.next_safe_timestamp();
            if self.advance_if_due(next) {
                // A node that never waits still has to hear its sockets and its timers.
                self.runtime.poll_io();
                continue;
            }
            if done() {
                return;
            }

            let alarm_due = next.and_then(|timestamp| self.due(timestamp));
            if let Some((task, kind)) = self.runtime.wait_wake_until(alarm_due) {
                self.handle_wake(task, kind);
            }
        }
    }

    /// Apply the wake policy before polling a task.
    fn handle_wake(&self, task: TaskId, kind: WakeKind) {
        if self.runtime.finished(task) {
            return; // Ignore stale wakes from tasks that already finished.
        }
        if self.shutdown.get() == Stage::Done && self.user_tasks.borrow().contains(&task) {
            return; // Nothing but the shutdown function runs at or after the shutdown timestamp.
        }
        match kind {
            WakeKind::Physical => self.defer(task),
            WakeKind::Resume => {
                // Handle the case when a task is waiting on multiple futures. If any of these are physical, make the task physical.
                if self.foreign.release(task) {
                    // This task may observe its foreign result now, so the timestamp is physical.
                    self.physical.set(true);
                }
                self.runtime.poll(task);
            }
        }
    }

    /// Assign a physical resumption timestamp to a task woken by a foreign future.
    pub(in crate::node) fn defer(&self, task: TaskId) {
        self.foreign.defer(task, || {
            (
                self.mint_physical_timestamp(),
                self.runtime.waker_for(task, WakeKind::Resume),
            )
        });
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use std::task::Poll;
    use std::time::{Duration, Instant};

    use futures::channel::oneshot;
    use futures::future::{Either, select};

    use super::super::testing::{Frames, node_rig};
    use super::*;
    use crate::app::{Builder, InputPort};
    use crate::driver::Timer;
    use crate::node::{ExecutionMode, Node};
    use crate::runtime::Runtime;
    use crate::time::{MS, S, Time};

    /// Record whether a wait preserves the physical flag of its triggering input.
    fn wait_after(physical: bool) -> ((bool, bool), Frames) {
        let (_runtime, scheduler, sink) = node_rig();
        let (port, channel) = InputPort::local(&scheduler);
        let node = Node {
            scheduler: scheduler.clone(),
        };
        if physical {
            channel.push_physical(2 * MS, 7);
        } else {
            channel.push(2 * MS, 7);
        }
        channel.update_remote_ent(Time::FOREVER);

        let seen = Rc::new(Cell::new(None));
        let (out, task_scheduler) = (seen.clone(), scheduler.clone());
        scheduler.run_user(async move {
            port.wait().await;
            let armed_at = task_scheduler.is_physical();
            node.wait(1 * MS).await;
            out.set(Some((armed_at, task_scheduler.is_physical())));
        });
        (seen.get().unwrap(), sink.sent.borrow().clone())
    }

    #[test]
    fn a_wait_armed_at_a_physical_timestamp_elapses_physically() {
        let (flags, frames) = wait_after(true);
        assert_eq!(flags, (true, true));
        // Both frames are physical and promise no further logical events.
        assert_eq!(
            frames,
            vec![(2 * MS, Time::FOREVER, true), (3 * MS, Time::FOREVER, true)]
        );
    }

    #[test]
    fn a_wait_armed_at_a_logical_timestamp_stays_logical() {
        let (flags, frames) = wait_after(false);
        assert_eq!(flags, (false, false));
        assert_eq!(
            frames,
            vec![(2 * MS, 3 * MS, false), (3 * MS, Time::FOREVER, false)]
        );
    }

    fn send_later(value: i64) -> (oneshot::Receiver<i64>, std::thread::JoinHandle<()>) {
        let (done, result) = oneshot::channel();
        let worker = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(5));
            done.send(value).unwrap();
        });
        (result, worker)
    }

    #[test]
    fn a_foreign_future_resumes_at_a_fresh_physical_timestamp() {
        let (_runtime, scheduler, sink) = node_rig();
        let node = Node {
            scheduler: scheduler.clone(),
        };
        let (result, worker) = send_later(7);

        let seen = Rc::new(Cell::new(None));
        let (out, task_scheduler) = (seen.clone(), scheduler.clone());
        scheduler.run_user(async move {
            // Begin at a logical timestamp before awaiting the foreign result.
            let started = node.wait(1 * MS).await;
            // The foreign future uses the task's default Physical waker.
            let value = result.await.unwrap();
            out.set(Some((
                started,
                value,
                task_scheduler.now(),
                task_scheduler.is_physical(),
            )));
        });
        worker.join().unwrap();

        let (started, value, resumed, physical) = seen.get().unwrap();
        assert_eq!((started, value, physical), (1 * MS, 7, true));
        assert!(resumed > started);
        // A foreign result creates a physical timestamp without lowering the logical ENT.
        assert_eq!(
            *sink.sent.borrow(),
            vec![
                (1 * MS, Time::FOREVER, false),
                (resumed, Time::FOREVER, true)
            ]
        );
    }

    #[test]
    fn a_pending_foreign_future_does_not_hold_time() {
        let (_runtime, scheduler, sink) = node_rig();
        let ticker = Node {
            scheduler: scheduler.clone(),
        };
        let (done, result) = oneshot::channel::<i64>();

        let seen = Rc::new(Cell::new(None));
        let (out, task_scheduler) = (seen.clone(), scheduler.clone());
        scheduler.spawn(async move {
            let value = result.await.unwrap();
            out.set(Some((
                task_scheduler.now(),
                task_scheduler.is_physical(),
                value,
            )));
        });
        scheduler.run_user(async move {
            // Other tasks can advance logical time while this result is pending.
            ticker.wait(1 * MS).await;
            ticker.wait(1 * MS).await;
            done.send(7).unwrap(); // The waiting task still treats this completion as physical.
        });

        let (resumed, physical, value) = seen.get().unwrap();
        assert!(resumed > 2 * MS, "{resumed}");
        assert_eq!((physical, value), (true, 7));
        assert_eq!(
            *sink.sent.borrow(),
            vec![
                (1 * MS, 2 * MS, false),
                (2 * MS, Time::FOREVER, false),
                (resumed, Time::FOREVER, true)
            ]
        );
    }

    #[test]
    fn a_logical_event_reaching_a_deferred_task_makes_its_timestamp_physical() {
        let (runtime, scheduler, sink) = node_rig();
        let (port, channel) = InputPort::local(&scheduler);
        let (result, worker) = send_later(7);
        // The later-arriving input has an earlier timestamp than the deferred foreign resumption.
        // Its thread waits for the worker, so the input arrives after the foreign result
        // however late the worker runs.
        let (after, then) = oneshot::channel::<()>();
        let later = std::thread::spawn(move || {
            worker.join().unwrap();
            std::thread::sleep(Duration::from_millis(15));
            after.send(()).unwrap();
        });
        runtime.spawn_internal(async move {
            then.await.unwrap();
            channel.push(1 * MS, 5);
            channel.update_remote_ent(Time::FOREVER);
        });

        let seen = Rc::new(Cell::new(None));
        let (out, task_scheduler) = (seen.clone(), scheduler.clone());
        scheduler.run_user(async move {
            let arrived = match select(port.wait(), result).await {
                Either::Left((value, _)) => *value.unwrap().0,
                Either::Right(_) => unreachable!("the value is delivered first"),
            };
            out.set(Some((
                arrived,
                task_scheduler.now(),
                task_scheduler.is_physical(),
            )));
        });
        later.join().unwrap();

        // The task can observe its foreign result at the input's timestamp.
        assert_eq!(seen.get(), Some((5, 1 * MS, true)));
        // The input wake cancels the task's separate physical resumption timestamp.
        assert_eq!(*sink.sent.borrow(), vec![(1 * MS, Time::FOREVER, true)]);
        assert_eq!(scheduler.now(), 1 * MS);
    }

    /// Yield once by requesting another poll.
    async fn yield_once() {
        let mut yielded = false;
        std::future::poll_fn(|cx| {
            if yielded {
                return Poll::Ready(());
            }
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        })
        .await
    }

    #[test]
    fn the_node_advances_only_once_no_task_can_run() {
        let (runtime, scheduler, _sink) = node_rig();
        let node = Node {
            scheduler: scheduler.clone(),
        };
        let timestamps = Rc::new(RefCell::new(Vec::new()));
        for _ in 0..2 {
            let (timestamps, scheduler) = (timestamps.clone(), scheduler.clone());
            runtime.spawn_internal(async move {
                timestamps.borrow_mut().push(scheduler.now());
                yield_once().await; // A queued wake keeps the current timestamp open.
                timestamps.borrow_mut().push(scheduler.now());
            });
        }
        let woke_at = Rc::new(Cell::new(None));
        let out = woke_at.clone();
        scheduler.run_user(async move { out.set(Some(node.wait(1 * MS).await)) });
        // Every runnable task finished before logical time advanced.
        assert_eq!(*timestamps.borrow(), vec![Time::ZERO; 4]);
        assert_eq!(woke_at.get(), Some(1 * MS));
    }

    #[test]
    fn tasks_woken_at_one_timestamp_run_in_the_order_they_were_spawned() {
        let (_runtime, scheduler, _sink) = node_rig();
        let (first, first_channel) = InputPort::local(&scheduler);
        let (second, second_channel) = InputPort::local(&scheduler);
        for channel in [&first_channel, &second_channel] {
            channel.push(1 * MS, 0);
            channel.update_remote_ent(Time::FOREVER);
        }
        let node = Node {
            scheduler: scheduler.clone(),
        };
        let order = Rc::new(RefCell::new(Vec::new()));

        // The node wakes the task of its alarm first, then the tasks of its inputs as created.
        let out = order.clone();
        scheduler.spawn(async move {
            second.wait().await;
            out.borrow_mut().push("second input");
        });
        let out = order.clone();
        scheduler.spawn(async move {
            first.wait().await;
            out.borrow_mut().push("first input");
        });
        let out = order.clone();
        scheduler.run_user(async move {
            node.wait(1 * MS).await;
            out.borrow_mut().push("alarm");
        });
        assert_eq!(*order.borrow(), ["second input", "first input", "alarm"]);
    }

    #[test]
    fn every_due_timestamp_is_completed_before_the_node_waits() {
        let (_runtime, scheduler, sink) = node_rig();
        let (_port, channel) = InputPort::local(&scheduler);
        channel.push(1 * S, 1); // Delivery must advance time even when no task is waiting.
        channel.push(2 * S, 2);
        channel.update_remote_ent(Time::FOREVER);
        let (result, worker) = send_later(7);
        scheduler.run_user(async move {
            result.await.unwrap();
        });
        worker.join().unwrap();

        // Both queued timestamps finish before the foreign result arrives.
        let frames = sink.sent.borrow();
        assert_eq!(
            frames[..2],
            [(1 * S, 2 * S, false), (2 * S, Time::FOREVER, false)]
        );
        // The foreign result resumes after both queued timestamps.
        assert!(frames[2].0 > 2 * S, "{}", frames[2].0);
    }

    #[test]
    fn a_node_that_never_waits_still_fires_its_timers() {
        let (runtime, scheduler, _sink) = node_rig();
        let node = Node {
            scheduler: scheduler.clone(),
        };
        let stop = scheduler.clone();
        runtime.spawn_internal(async move {
            Timer::after(Duration::from_millis(20)).await;
            stop.shut_down_now();
        });
        let start = Instant::now();
        scheduler.run_user(async move {
            // In fast-forward mode the next timestamp is always due, so the node never waits.
            while start.elapsed() < Duration::from_secs(10) {
                node.wait(1 * MS).await;
            }
        });
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the timer never fired"
        );
    }

    #[test]
    fn a_foreign_wake_of_an_internal_task_runs_at_once() {
        let (runtime, scheduler, _sink) = node_rig();
        let (result, worker) = send_later(7);
        let seen = Rc::new(Cell::new(None));
        let (out, task_scheduler) = (seen.clone(), scheduler.clone());
        let (done, finished) = oneshot::channel();
        runtime.spawn_internal(async move {
            let value = result.await.unwrap();
            out.set(Some((
                value,
                task_scheduler.now(),
                task_scheduler.is_physical(),
            )));
            done.send(()).unwrap();
        });
        scheduler.run(async move { finished.await.unwrap() }, Vec::new());
        worker.join().unwrap();
        // Internal tasks resume without creating a new timestamp.
        assert_eq!(seen.get(), Some((7, Time::ZERO, false)));
    }

    #[test]
    fn a_resume_waker_woken_from_a_thread_is_not_deferred() {
        let (_runtime, scheduler, _sink) = node_rig();
        let seen = Rc::new(Cell::new(None));
        let (out, task_scheduler) = (seen.clone(), scheduler.clone());
        scheduler.run_user(async move {
            let mut handed_out = false;
            std::future::poll_fn(|cx| {
                if handed_out {
                    return Poll::Ready(());
                }
                handed_out = true;
                let resume = Runtime::waker(cx, WakeKind::Resume);
                std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_millis(5));
                    resume.wake();
                });
                Poll::Pending
            })
            .await;
            out.set(Some((task_scheduler.now(), task_scheduler.is_physical())));
        });
        // Resume wakes poll at the current timestamp without physical deferral.
        assert_eq!(seen.get(), Some((Time::ZERO, false)));
    }

    #[test]
    fn builder_execution_mode_controls_wait_pacing() {
        let runtime = Rc::new(Runtime::new());
        let scheduler = Rc::new(NodeScheduler::new(runtime));
        let mut builder = Builder::new(0, scheduler.clone());
        let node = builder.node();
        let seen = Rc::new(Cell::new(None));
        let out = seen.clone();
        let target = 3600 * S;
        scheduler.spawn(async move {
            out.set(Some(node.wait(target).await));
        });

        scheduler.run_until_idle();
        assert_eq!(seen.get(), None); // Realtime mode waits for the wall clock.
        assert_eq!(scheduler.now(), Time::ZERO);

        builder.execution_mode(ExecutionMode::FastForward);
        scheduler.run_until_idle();
        assert_eq!(seen.get(), Some(target)); // FastForward mode removes the wall-clock wait.
        assert_eq!(scheduler.now(), target);
    }

    #[test]
    fn a_realtime_node_sleeps_until_the_timestamp() {
        let (_runtime, scheduler, _sink) = node_rig();
        scheduler.execution_mode.set(ExecutionMode::Realtime);
        let node = Node {
            scheduler: scheduler.clone(),
        };
        let start = Instant::now();
        scheduler.run_user(async move {
            node.wait(5 * MS).await;
        });
        assert!(start.elapsed() >= Duration::from_millis(5));
    }

    #[test]
    fn an_unrelated_advance_does_not_absorb_a_deferred_task() {
        let (runtime, scheduler, _sink) = node_rig();
        let (a, a_channel) = InputPort::local(&scheduler);
        let (_b, b_channel) = InputPort::local(&scheduler);
        // This input has no logical events but remains open.
        a_channel.update_remote_ent(Time::FOREVER);
        // The result arrives after 5ms and gets a physical resumption timestamp.
        let (result, worker) = send_later(7);
        // B arrives later in wall-clock time but has an earlier logical timestamp.
        let (after, then) = oneshot::channel::<()>();
        let later = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(15));
            after.send(()).unwrap();
        });
        runtime.spawn_internal(async move {
            then.await.unwrap();
            b_channel.push(3 * MS, 1);
            b_channel.update_remote_ent(Time::FOREVER);
        });

        let seen = Rc::new(Cell::new(None));
        let (out, task_scheduler) = (seen.clone(), scheduler.clone());
        scheduler.run_user(async move {
            let value = match select(a.wait(), result).await {
                Either::Left(_) => unreachable!("A never delivers"),
                Either::Right((value, _)) => value.unwrap(),
            };
            out.set(Some((
                value,
                task_scheduler.now(),
                task_scheduler.is_physical(),
            )));
        });
        worker.join().unwrap();
        later.join().unwrap();

        let (value, resumed_at, physical) = seen.get().unwrap();
        assert_eq!((value, physical), (7, true));
        assert!(
            resumed_at > 3 * MS,
            "resumed at {resumed_at}: B's timestamp, where nothing of A's arrived"
        );
    }

    #[test]
    fn await_physical_continues_physically_even_when_already_complete() {
        let (_runtime, scheduler, sink) = node_rig();
        let node = Node {
            scheduler: scheduler.clone(),
        };
        let (done, result) = oneshot::channel::<i64>();
        // The future is already ready, so awaiting it will not register a wake.
        done.send(7).unwrap();
        let seen = Rc::new(Cell::new(None));
        let (out, task_scheduler) = (seen.clone(), scheduler.clone());
        scheduler.run_user(async move {
            node.wait(1 * MS).await;
            let value = node.await_physical(result).await.unwrap();
            out.set(Some((
                value,
                task_scheduler.now(),
                task_scheduler.is_physical(),
            )));
        });
        let (value, at, physical) = seen.get().unwrap();
        assert_eq!((value, physical), (7, true));
        assert!(at > 1 * MS, "{at}");
        assert_eq!(
            *sink.sent.borrow(),
            vec![(1 * MS, Time::FOREVER, false), (at, Time::FOREVER, true)]
        );
    }

    #[test]
    fn a_task_spawned_by_a_task_keeps_the_node_running() {
        let (_runtime, scheduler, sink) = node_rig();
        let node = Rc::new(Node {
            scheduler: scheduler.clone(),
        });
        let seen = Rc::new(Cell::new(None));
        let (out, parent) = (seen.clone(), node.clone());
        scheduler.run_user(async move {
            let child = parent.clone();
            parent.spawn(async move { out.set(Some(child.wait(2 * MS).await)) });
            parent.wait(1 * MS).await;
        });
        assert_eq!(seen.get(), Some(2 * MS));
        // The outputs stay open until the spawned task finishes.
        assert_eq!(
            *sink.sent.borrow(),
            vec![(1 * MS, 2 * MS, false), (2 * MS, Time::FOREVER, false)]
        );
        assert!(sink.closed.get());
    }

    #[test]
    fn an_aborted_node_stops_without_completing_its_timestamp() {
        let (_runtime, scheduler, sink) = node_rig();
        let node = Node {
            scheduler: scheduler.clone(),
        };
        let late = Rc::new(Cell::new(false));
        let (out, other) = (late.clone(), scheduler.clone());
        scheduler.spawn(async move {
            Node { scheduler: other }.wait(5 * MS).await;
            out.set(true);
        });
        scheduler.run_user(async move {
            node.wait(1 * MS).await;
            assert!(!node.timestamp_complete());
            node.abort();
        });
        assert_eq!(scheduler.now(), 1 * MS);
        assert!(!late.get()); // The other task never ran again.
        assert!(sink.sent.borrow().is_empty());
        assert!(!sink.closed.get());
    }
}
