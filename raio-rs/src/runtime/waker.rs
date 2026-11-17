use super::{TaskId, WakeKind, WakeSender};
use std::sync::Arc;
use std::task::{Context, RawWaker, RawWakerVTable, Waker};

/// The task ID, scheduling policy, and queue used by a runtime waker.
struct TaskWaker {
    task_id: TaskId,
    kind: WakeKind,
    wake_sender: WakeSender,
}

impl TaskWaker {
    fn wake(&self) {
        self.wake_sender.send((self.task_id, self.kind));
    }
}

const _: () = {
    const fn sendable_from_any_thread<T: Send + Sync>() {}
    sendable_from_any_thread::<TaskWaker>()
};

/// Identifies Raio wakers and manages their shared task data.
static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_waker, wake, wake_by_ref, drop_waker);

/// Create a waker with one owned Arc reference to the task data.
pub(super) fn new(task_id: TaskId, kind: WakeKind, wake_sender: WakeSender) -> Waker {
    let data = Arc::into_raw(Arc::new(TaskWaker {
        task_id,
        kind,
        wake_sender,
    }));
    // SAFETY: data is a live Arc<TaskWaker> pointer managed by the vtable callbacks.
    unsafe { Waker::new(data as *const (), &VTABLE) }
}

// SAFETY: data comes from Arc::into_raw; each waker owns one TaskWaker reference.
unsafe fn clone_waker(data: *const ()) -> RawWaker {
    unsafe { Arc::increment_strong_count(data as *const TaskWaker) };
    RawWaker::new(data, &VTABLE)
}

unsafe fn wake(data: *const ()) {
    unsafe { Arc::from_raw(data as *const TaskWaker) }.wake();
}

unsafe fn wake_by_ref(data: *const ()) {
    unsafe { &*(data as *const TaskWaker) }.wake();
}

unsafe fn drop_waker(data: *const ()) {
    drop(unsafe { Arc::from_raw(data as *const TaskWaker) });
}

/// Return task data only for wakers created by this module.
fn task_waker(waker: &Waker) -> Option<&TaskWaker> {
    // SAFETY: The vtable identifies TaskWaker data kept alive by the borrowed waker.
    std::ptr::eq(waker.vtable(), &VTABLE).then(|| unsafe { &*(waker.data() as *const TaskWaker) })
}

/// Read the task ID from a Raio context; other waker implementations cause a panic.
pub(super) fn current_task(cx: &Context<'_>) -> TaskId {
    task_waker(cx.waker())
        .expect("Runtime::current_task is only for futures a raio runtime polls")
        .task_id
}

/// Create a waker for the same task with a different scheduling policy.
pub(super) fn with_kind(cx: &Context<'_>, kind: WakeKind) -> Waker {
    let current =
        task_waker(cx.waker()).expect("Runtime::waker is only for futures a raio runtime polls");
    new(current.task_id, kind, current.wake_sender.clone())
}
