---
sidebar_position: 5
---

# Failure and Shutdown

The [Shutdown](../tutorial/shutdown.md) tutorial shows what follows from the side of a program.

## How an App Ends

An app ends in one of these ways:

- **Completion**: every node has finished its tasks.
- **Shutdown request**: a node calls `request_shutdown`, and every node shuts down at one timestamp.
- **Interrupt**: the user presses Ctrl-C, and every node shuts down at once.
- **Failure**: a node fails, and the coordinator shuts down the others.

In each case, every node that has a shutdown method runs it once.

## Heartbeats

The coordinator has to know that each node is alive. A running node sends it a **heartbeat** twice a second. A node whose process exits with an error has failed, and so has a node from which the coordinator has not heard for five seconds.

It follows that nothing in a node may block its thread for that long: not a task, a reaction, or its shutdown method. Blocking work belongs in a thread that reports through an action.

## Failure

When a node fails, the coordinator shuts down the other nodes, and the launcher reports the node that failed. An app does not carry on without one of its nodes.

A node that loses its coordinator shuts down at once, on its own.

## Closed Ports

When a node finishes on its own, its output ports **close**. A receiver sees this on the connected input port: a loop over the port's values ends, and a reaction stops once every port that triggers it has closed. In Python, `await port` on a closed port raises `raio.Closed`; in Rust, `wait()` returns `None`.
