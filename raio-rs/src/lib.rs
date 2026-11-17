//! raio is a deterministic realtime runtime for apps made of nodes in separate processes.
//!
//! A node's tasks set values on output ports, and wait for values on input ports and for logical
//! time to pass. Every value has a timestamp, a point in logical time, and a node moves to a
//! timestamp only once none of its inputs can still bring an earlier value. So every run of an
//! app sees the same events in the same order, however fast each process happens to be. What
//! comes from outside the app, such as an [`app::Action`] or the future of another runtime, gets
//! its timestamp from the wall clock and is marked physical.
//!
//! ```no_run
//! use raio::app::{App, Builder, InputPort, Node, OutputPort};
//! use raio::tasks;
//! use raio::time::MS;
//! use std::rc::Rc;
//!
//! struct Producer {
//!     node: Node,
//!     out: OutputPort<String>,
//! }
//!
//! impl Producer {
//!     fn new(builder: &mut Builder) -> Self {
//!         Self {
//!             node: builder.node(),
//!             out: builder.output(),
//!         }
//!     }
//!
//!     async fn start(self: Rc<Self>) {
//!         self.node.wait(1 * MS).await;
//!         self.out.set("hello world".to_string());
//!     }
//! }
//!
//! struct Sink {
//!     input: InputPort<String>,
//! }
//!
//! impl Sink {
//!     fn new(builder: &mut Builder) -> Self {
//!         Self {
//!             input: builder.input(),
//!         }
//!     }
//!
//!     async fn start(self: Rc<Self>) {
//!         while let Some((value, timestamp)) = self.input.wait().await {
//!             println!("sink received {:?} at {timestamp}", *value);
//!         }
//!     }
//! }
//!
//! let mut app = App::new();
//! let producer = app.create_node(Producer::new, tasks![Producer::start]);
//! let sink = app.create_node(Sink::new, tasks![Sink::start]);
//! app.connect(&producer.out, &sink.input);
//! app.run();
//! ```
//!
//! # Modules
//!
//! - [`app`]: the building blocks of a node program, and the `App` that connects and launches
//!   nodes.
//! - [`time`]: timestamps and durations.
//! - [`serializer`] and [`transport`]: how a connection encodes values and carries them.
//! - [`node`]: the scheduler that orders the events of one node by timestamp.
//! - [`coordinator`] and [`messages`]: the graph of an app, and how its nodes are wired and shut
//!   down together.
//! - [`runtime`] and [`driver`]: the single-threaded executor under each node, and its I/O and
//!   timers.

pub mod app;
pub mod coordinator;
pub mod driver;
pub mod messages;
pub mod node;
pub mod runtime;
pub mod serializer;
pub mod time;
pub mod transport;
