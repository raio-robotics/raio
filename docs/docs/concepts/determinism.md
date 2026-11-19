---
sidebar_position: 2
---

# Determinism

An app in Raio is **deterministic**: every run sees the same events at the same timestamps in the same order, however fast or slow each process happens to be.

## Why It Matters

A robot's software is many processes that run at different speeds. If the result depends on which message happened to arrive first, a bug may show once in a thousand runs and never under a debugger. With determinism, a run can be repeated: the same inputs give the same behavior, in a test as on the robot.

## Promises

Every value carries the timestamp at which its sender set it. That alone is not enough, because a receiver must also know that nothing earlier is still on its way.

So with every timestamp, a node gives each of its receivers a **promise**: the earliest timestamp at which it may send again. A receiver handles a timestamp only once the promise of each of its senders is beyond that timestamp. It therefore never handles a timestamp while an earlier value could still arrive.

## The First Promise

Before a sender has made its first promise, a receiver has one that the coordinator works out from the graph: nothing arrives over a connection until the delay of that connection has passed.

This is what lets a loop of connections start. The loop needs a delay, and one of its nodes has to act before that delay has passed.

## Where Determinism Ends

Some events come from outside the app, and their timestamps come from the wall clock:

- A value sent to an **action** gets the time at which it arrives.
- A value sent over a **physical connection** gets the time of the receiver's clock when it arrives.
- An **interrupt** (Ctrl-C) stops the nodes at once, at different timestamps.

The timestamps of such **physical events** differ from one run to the next. A node can tell that it is at one: `is_physical()` is then true. What follows from a physical event is deterministic again. Given the same events at the same timestamps, the app does the same.
