//! Connections between nodes: a node registers its ports, and then each output sends one frame
//! per timestamp to every input that it feeds.

use std::collections::HashMap;
use std::rc::Rc;

use futures::channel::mpsc::unbounded;
use futures::future::join_all;

use super::port::{InputChannel, OutputChannel};
use super::setting::PortSetting;
use crate::coordinator::Link;
use crate::messages::{Downstream, Frame, InputInfo, OutputInfo, RegisterNode, Reply};
use crate::node::NodeScheduler;
use crate::runtime::BoxFuture;
use crate::serializer::Serializer;
use crate::serializer::bincode::BincodeSerializer;
use crate::time::Time;

/// An input port whose payload type is hidden from the node's wiring.
pub(crate) trait AnyInput {
    /// Listen with the port's transport; return its description and the task that receives frames.
    fn listen(
        self: Rc<Self>,
        scheduler: Rc<NodeScheduler>,
    ) -> BoxFuture<(InputInfo, BoxFuture<()>)>;
}

/// An output port whose payload type is hidden from the node's wiring.
pub(crate) trait AnyOutput {
    fn describe(&self) -> OutputInfo;
    /// Attach a sender for one downstream input and return the task that sends its frames.
    fn connect_to(self: Rc<Self>, downstream: Downstream) -> BoxFuture<()>;
}

/// Build the frame that one connection carries for a timestamp, at the receiver's times.
fn make_frame<T>(
    serializer: &dyn Serializer<T>,
    delay: Time,
    timestamp: Time,
    ent: Time,
    physical: bool,
    value: Option<&T>,
) -> Frame {
    Frame {
        timestamp: timestamp + delay,
        ent: ent + delay,
        physical,
        value: value.map(|value| serializer.serialize(value)),
    }
}

/// Queue the frame's value and update the input's ENT.
fn deliver<T>(
    serializer: &dyn Serializer<T>,
    channel: &InputChannel<T>,
    scheduler: &NodeScheduler,
    message: Frame,
) {
    if let Some(bytes) = message.value {
        let value = serializer.deserialize(&bytes);
        if message.physical {
            // Move late physical events to a timestamp after the node's current timestamp.
            let timestamp = message.timestamp.max(scheduler.now().next_microstep());
            channel.push_physical(timestamp, value);
        } else {
            channel.push(message.timestamp, value);
        }
    }
    channel.update_remote_ent(message.ent);
}

/// An input port is its channel, which takes the frames, and its setting.
impl<T: 'static> AnyInput for (Rc<InputChannel<T>>, Rc<PortSetting<T>>) {
    fn listen(
        self: Rc<Self>,
        scheduler: Rc<NodeScheduler>,
    ) -> BoxFuture<(InputInfo, BoxFuture<()>)> {
        Box::pin(async move {
            let (channel, setting) = (self.0.clone(), &self.1);
            let (serializer_name, serializer) = setting.serializers.first();
            let (transport_name, transport) = setting.transports.first();
            let (endpoint, accept) = transport.listen().await;
            let info = InputInfo {
                id: setting.id.port_id,
                serializer: serializer_name,
                transport: transport_name,
                endpoint,
            };
            let receive: BoxFuture<()> = Box::pin(async move {
                let input = channel.clone();
                accept(Box::new(move |bytes| {
                    let message: Frame = BincodeSerializer.deserialize(&bytes);
                    deliver(&*serializer, &input, &scheduler, message);
                }))
                .await;
                channel.close();
            });
            (info, receive)
        })
    }
}

impl<T: 'static> AnyOutput for OutputChannel<T> {
    fn describe(&self) -> OutputInfo {
        self.setting.describe()
    }

    fn connect_to(self: Rc<Self>, downstream: Downstream) -> BoxFuture<()> {
        let Downstream { input, delay } = downstream;
        let serializer = self.setting.serializers.get(&input.serializer);
        let transport = self.setting.transports.get(&input.transport);
        let (frames, queued) = unbounded::<Vec<u8>>();
        self.senders
            .borrow_mut()
            .push(Box::new(move |timestamp, ent, physical, value| {
                let message = make_frame(&*serializer, delay, timestamp, ent, physical, value);
                frames
                    .unbounded_send(BincodeSerializer.serialize(&message))
                    .unwrap();
            }));
        transport.send(input.endpoint, queued)
    }
}

/// Register the node's ports and return the task that transfers frames over all connections,
/// or `None` if the app does not start.
pub(crate) async fn connect_node(
    scheduler: Rc<NodeScheduler>,
    mut link: Link,
    inputs: Vec<Rc<dyn AnyInput>>,
    outputs: Vec<Rc<dyn AnyOutput>>,
) -> Option<BoxFuture<()>> {
    let mut registration = RegisterNode {
        inputs: Vec::new(),
        outputs: Vec::new(),
    };
    let mut receives = Vec::new();
    for input in inputs {
        let (info, receive) = input.listen(scheduler.clone()).await;
        registration.inputs.push(info);
        receives.push(receive);
    }
    let mut senders = HashMap::new();
    for output in outputs {
        let info = output.describe();
        senders.insert(info.id, output);
        registration.outputs.push(info);
    }

    let wiring = match link.register(&registration).await {
        Reply::Wired(wiring) => wiring,
        Reply::Refused(refusal) => panic!("node {}: {refusal}", link.node_id),
        Reply::Cancelled => return None,
    };
    scheduler.start_clock();
    // The link stays open while the node runs; the node does not wait for it.
    scheduler
        .runtime
        .spawn_internal(link.run(scheduler.clone()));

    let mut sends = Vec::new();
    for (id, downstreams) in wiring {
        for downstream in downstreams {
            sends.push(senders[&id].clone().connect_to(downstream));
        }
    }
    Some(Box::pin(async move {
        futures::join!(join_all(sends), join_all(receives));
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::EventSource;
    use crate::runtime::Runtime;
    use crate::time::{MS, S};

    /// Create a node at logical time 5s and an input channel for a connection to deliver to.
    fn node_at_5s() -> (Rc<NodeScheduler>, InputChannel<i64>) {
        let scheduler = Rc::new(NodeScheduler::new(Rc::new(Runtime::new())));
        scheduler.advance_to(5 * S);
        (scheduler, InputChannel::new())
    }

    fn receive(
        scheduler: &NodeScheduler,
        channel: &InputChannel<i64>,
        timestamp: Time,
        ent: Time,
        physical: bool,
    ) {
        // The sender adds the connection's delay before the frame travels.
        let message = make_frame(
            &BincodeSerializer,
            10 * MS,
            timestamp,
            ent,
            physical,
            Some(&7),
        );
        deliver(&BincodeSerializer, channel, scheduler, message);
    }

    #[test]
    fn a_logical_frame_keeps_its_timestamp_plus_the_delay() {
        let (scheduler, channel) = node_at_5s();
        receive(&scheduler, &channel, 7 * S, 6 * S, false);
        assert_eq!(channel.next_event_timestamp(), Some(7 * S + 10 * MS));
        assert_eq!(channel.ent(), 6 * S + 10 * MS);
    }

    #[test]
    fn a_physical_frame_from_this_nodes_past_is_moved_right_after_now() {
        let (scheduler, channel) = node_at_5s();
        // The delayed sender timestamp is behind this node's clock.
        receive(&scheduler, &channel, 1 * S, 6 * S, true);
        assert_eq!(
            channel.next_event_timestamp(),
            Some((5 * S).next_microstep())
        );
        // Moving a physical value to a later timestamp does not change its ENT promise.
        assert_eq!(channel.ent(), 6 * S + 10 * MS);
    }

    #[test]
    fn a_physical_frame_keeps_the_senders_timestamp_plus_the_delay() {
        let (scheduler, channel) = node_at_5s();
        receive(&scheduler, &channel, 7 * S, 20 * S, true);
        assert_eq!(channel.next_event_timestamp(), Some(7 * S + 10 * MS));
    }

    #[test]
    fn a_connection_delivers_in_the_order_it_was_sent() {
        let (scheduler, channel) = node_at_5s();
        // A late physical value moves to (5s, 1).
        receive(&scheduler, &channel, 1 * S, 6 * S, true);
        // The next late value follows at (5s, 2).
        receive(&scheduler, &channel, 2 * S, 6 * S, true);
        // The logical value follows both physical values.
        receive(&scheduler, &channel, 6 * S, 20 * S, false);
        let after_now = (5 * S).next_microstep();
        for expected in [after_now, after_now.next_microstep(), 6 * S + 10 * MS] {
            assert_eq!(channel.next_event_timestamp(), Some(expected));
            channel.wake_wakers(expected);
        }
        assert_eq!(channel.next_event_timestamp(), None);
    }
}
