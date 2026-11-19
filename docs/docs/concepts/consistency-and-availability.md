---
sidebar_position: 4
---

# Consistency and Availability

A distributed system cannot always be both consistent and available. Raio lets you choose between the two for each connection.

## The Trade-Off

A connection keeps an app **consistent**: a receiver never runs ahead of what its senders have promised, so the order of events is the same on every run. The price is **availability**: a node that waits for a slow sender does not advance.

Each of the following gives up some of one for the other.

## Connections

A plain connection is fully consistent. The receiver sees the same events at the same timestamps on every run, and it waits for its sender to finish each timestamp before it handles that timestamp itself.

## Delays

A connection with a **delay** is still deterministic, but the receiver may run ahead of its sender by up to the delay. The cost is that each value arrives that much later in logical time.

A delay suits a slow sender that feeds a fast loop. Suppose a planner takes tens of milliseconds to compute and feeds a control loop that must not stall. A delay on the planner's output, longer than the planner's slowest step, lets the control loop keep its rate. The control loop then acts on a plan that is one delay old.

## Physical Connections

A **physical connection** is fully available: the receiver never waits for the sender. Each value gets its timestamp from the receiver's wall clock when it arrives, so the timestamps differ between runs.

A physical connection suits input that should never hold a node back, such as an operator's commands to a controller.

## Actions

An **action** is handled when its value arrives, and nothing waits for it. Like a physical connection, it gives up determinism for its own timestamp, which comes from the wall clock.

## Shutdown

The same choice appears when an app shuts down. A **shutdown request** is consistent: every node shuts down at one timestamp, and the nodes run on until that timestamp. An **interrupt** is available: every node shuts down at once, and the nodes stop at different timestamps.
