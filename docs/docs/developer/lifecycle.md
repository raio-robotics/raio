---
sidebar_position: 2
---

# Lifecycle

## Startup

Startup goes through these steps:

1. The launcher starts the coordinator and one process per node.
2. Each node connects to the coordinator and gets its **code**. A Python node is rebuilt from it at this point.
3. Each node registers its ports and gets its **wiring**: which of its ports connect to which nodes.
4. Each node runs, and sends the coordinator a heartbeat twice a second.

The app starts only once every node is wired. Setup that a node does while it is rebuilt, such as loading a model, therefore delays the start for all nodes, and no node's logical clock runs in the meantime.

## Execution

Once it runs, a node repeats one step. Its scheduler picks the earliest timestamp with an event: a value on an input, a wait that is over, or a value on an action. It then waits until the promise of every sender is beyond that timestamp, and until the wall clock has reached it. In fast-forward mode it does not wait for the wall clock.

The scheduler then runs the tasks and reactions that the events wake, in the order in which they are declared. When none of them can go on, the node sends what its outputs hold, together with a new promise: the earliest timestamp at which it may send again.

## Shutdown

Every shutdown of an app comes from its coordinator. There are three triggers.

When a node calls `request_shutdown`, the coordinator picks a timestamp after the latest of the nodes' current timestamps, and every node shuts down at that timestamp.

When the user presses Ctrl-C in the launcher, the coordinator tells every node to shut down at once.

When a node's process exits with an error, or the coordinator has not heard from it for five seconds, the coordinator tells the other nodes to shut down at once, and the launcher reports the node.

A node that loses its coordinator shuts down on its own.
