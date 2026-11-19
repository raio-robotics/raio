---
sidebar_position: 1
---

# Model of Computation

Raio's model of computation is close to the reactor model of [Lingua Franca](https://www.lf-lang.org): components exchange timestamped events, and the order of everything is decided by logical time.

## Nodes

A **node** is a unit of computation with its own process, its own ports, and its own logical clock. Nodes share no memory. An **app** is a set of nodes and the connections between them.

## Timestamps

Every event in an app has a **timestamp**: a point in logical time. A timestamp counts the nanoseconds since the app started, and it carries a **microstep** that orders events within one nanosecond. See [Logical and Physical Time](time.md).

## Ports and Connections

An **output port** holds at most one value per timestamp and sends it when the timestamp completes. An **input port** receives the values of exactly one output port. A **connection** joins the two: each value arrives at the sender's timestamp plus the **delay** of the connection, which is zero unless one is given. One output may feed any number of inputs.

## Tasks and Reactions

A **task** is an async function that starts with its node. It waits for values and for logical time to pass, and it sets outputs. A **reaction** is a function that runs once at each timestamp at which one of its input ports or actions has a value.

## Actions

An **action** is an input that takes no connection. Threads of the node's own program feed it, for example a sensor driver. Its values are **physical events**: their timestamps come from the wall clock.

## The Coordinator

The **coordinator** runs in the process that launches the app. It wires the nodes together at startup, hears from each node that it is alive, and is the one that shuts the app down. Port values do not pass through it.

## How a Node Runs

A node moves through logical time one timestamp after another, and never goes back. At each step it moves to the earliest timestamp at which it has something to do. The tasks and reactions with something to do then run one after another, in the order in which they are declared. When none of them can go on, the timestamp is **complete**, and the node sends what its outputs hold.
