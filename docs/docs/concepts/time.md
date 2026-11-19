---
sidebar_position: 3
---

# Logical and Physical Time

Raio has two notions of time: logical time and physical time.

## Logical Time

**Logical time** is the time by which an app orders its events. Each node has a logical clock, whose value is the timestamp the node is handling. It starts at zero when the app starts, and it moves from one event to the next: it does not advance while a task or a reaction runs. A node reads it with `raio.now()` in Python and `node.now()` in Rust.

## Physical Time

**Physical time** is the time of the wall clock. A node reads it with `raio.physical_now()` in Python and `node.physical_now()` in Rust, which give the time since the node started.

## Lag

By default, logical time keeps pace with physical time: a node handles a timestamp no earlier than the wall clock reaches it. A wait of one second of logical time therefore takes one second.

The **lag** of a node is physical time minus logical time. It is small when the node keeps up. A node falls behind when a reaction is slow, or when it waits for a slow sender. A node that is behind skips nothing: it handles each timestamp in turn, as fast as it can, until it has caught up.

## Fast-Forward

In **fast-forward** mode, a node does not wait for the wall clock. The timestamps are the same, and so are the results, but the app runs as fast as it can. This suits tests and replays. In Python it is `raio.run(*nodes, fast_forward=True)`.

## Microsteps

A timestamp is a pair of a time in nanoseconds and a **microstep**. The microstep orders events that happen at the same nanosecond. For example, a shutdown is requested at `(3ms, 1)`, one microstep after `3ms`, so that it follows everything else that happens at `3ms`.

## Time Values

A time value is a number with a unit: `NS`, `US`, `MS`, `S` (or `SECOND`), `MINUTE`, or `HOUR`. For example, `20 * MS` is twenty milliseconds. The result is a `Time`, the type of both timestamps and durations. `Time.ZERO` is zero, `Time.NEVER` is before every timestamp, and `Time.FOREVER` is after every timestamp.
