use crate::time::Time;

/// Supplies events and ENT promises so the scheduler can order timestamps safely.
pub trait EventSource {
    /// Earliest Next Timestamp (ENT): no logical event may arrive at a smaller timestamp.
    fn ent(&self) -> Time;

    /// The next deliverable timestamp, or `None` if unknown; physical time may still lag behind.
    fn next_event_timestamp(&self) -> Option<Time>;

    /// Deliver events through `time`, wake their tasks, and report whether any event was physical.
    fn wake_wakers(&self, time: Time) -> bool;
}

/// Receives completed timestamp frames and closes when the node finishes producing output.
pub trait EventSink {
    fn send(&self, timestamp: Time, ent: Time, physical: bool);
    fn close(&self);
}
