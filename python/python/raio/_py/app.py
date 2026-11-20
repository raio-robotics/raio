"""Connecting ports and running an app: one process per node, wired by a coordinator."""

from __future__ import annotations

import os
import pickle
import subprocess
import sys
import time
from typing import TypeVar

import cloudpickle

from .._rs import core
from .._rs.core import Time
from . import states
from .node import Node, RustNode
from .ports import InputPort, OutputPort
from .serializers import MessagePack, Serializer
from .utils import end_processes, flush_output

T = TypeVar("T")

# (source, target, delay, serializer, physical) of every connection made in this process
_connections = []


def connect(
    source: OutputPort[T],
    target: InputPort[T],
    *,
    delay: Time = Time.ZERO,
    serializer: Serializer[T] | None = None,
    physical: bool = False,
) -> None:
    """Send every value of an output port to an input port, shifted by a logical delay.

    A physical connection gives each value a timestamp from the receiver's clock when it
    arrives, and the receiver never waits for the sender. It takes no delay.
    """
    if not isinstance(source, OutputPort) or source._node is None:
        raise ValueError(f"connect needs an output port of a node instance, got {source!r}")
    if not isinstance(target, InputPort) or target._node is None:
        raise ValueError(f"connect needs an input port of a node instance, got {target!r}")
    if not isinstance(delay, Time) or delay < Time.ZERO:
        raise ValueError("a connection delay is a Time of zero or more")
    _connections.append((source, target, delay, serializer, physical))


def run(*nodes: Node, fast_forward: bool = False) -> None:
    """Run each node in its own process until all of them finish."""
    if states.running is not None:
        raise RuntimeError("raio.run cannot be called from inside a node")
    connections, serializers = _wire(_node_ids("run", nodes))
    # The coordinator is 0, so the ids of the nodes count from 1.
    nodes = dict(enumerate(nodes, start=1))
    # Pickling comes first, so that a node that cannot be sent fails before anything starts.
    # Each node's process imports from the same places as this one, so the path goes with it.
    code = {}
    for node_id, node in nodes.items():
        if not isinstance(node, RustNode):  # A Rust node is a program of its own and has no code.
            sent = cloudpickle.dumps((node, serializers[node_id]))
            code[node_id] = pickle.dumps((sys.path, sent))
    coordinator = core.coordinator(len(nodes), connections, code)
    _launch(nodes, coordinator, fast_forward)


def diagram(path: str | os.PathLike, *nodes: Node) -> None:
    """Draw the nodes and their connections to an SVG file, in the style of Lingua Franca."""
    connections, _ = _wire(_node_ids("diagram", nodes))
    shown = []
    for node in nodes:
        ports = [(port._name, port._kind) for port in node._ports]
        tasks = []
        for name in node._tasks:
            # A reaction has the positions of its triggers; a task has none.
            triggers = getattr(type(node), name)._raio_triggers
            if triggers is not None:
                triggers = [getattr(node, trigger._name)._index for trigger in triggers]
            tasks.append((name, triggers))
        shown.append((type(node).__name__, ports, tasks))
    core.diagram(path, shown, connections)


def _node_ids(caller, nodes):
    """Map the identity of each node object of an app to the node's id, which counts from 1."""
    if not nodes:
        raise ValueError(f"{caller} needs at least one node")
    node_ids = {}
    for node in nodes:
        if not isinstance(node, Node):
            raise TypeError(f"{caller} takes node instances, got {node!r}")
        if id(node) in node_ids:
            raise ValueError(f"{type(node).__name__} was given to {caller} twice")
        node_ids[id(node)] = len(node_ids) + 1
    return node_ids


def _wire(node_ids):
    """List the connections among the nodes, and the serializers that each node's ports take.

    `node_ids` maps the identity of each node object of the app to the node's id.
    """
    connections = []
    # (port, name, serializer) for the ports of each node, by the node's id.
    serializers = {node_id: [] for node_id in node_ids.values()}
    for source, target, delay, serializer, physical in _connections:
        ends = (id(source._node) in node_ids, id(target._node) in node_ids)
        if ends == (False, False):
            continue  # A connection of another app.
        if ends != (True, True):
            raise ValueError(f"{source!r} feeds {target!r}, but only one of their nodes is run")
        source_node, target_node = node_ids[id(source._node)], node_ids[id(target._node)]
        connections.append(
            (source_node, source._index, target_node, target._index, delay, physical)
        )
        rust = [isinstance(port._node, RustNode) for port in (source, target)]
        if serializer is not None and all(rust):
            raise ValueError(f"{source!r} feeds {target!r}: Rust nodes choose their own serializer")
        if serializer is None and any(rust) and not all(rust):
            serializer = MessagePack()  # A Python node and a Rust node share no other serializer.
        if serializer is not None:
            # Both ends know the serializer by its name: the one it has, or that of its class.
            name = f"{type(serializer).__module__}.{type(serializer).__qualname__}"
            name = getattr(serializer, "name", name)
            # A Rust node sets its ports itself, so only a Python node takes what is listed here.
            serializers[source_node].append((source._index, name, serializer))
            serializers[target_node].append((target._index, name, serializer))
    return connections, serializers


def _launch(nodes, coordinator, fast_forward):
    """Start one process per node, which joins the app through the coordinator; wait for all.

    `nodes` maps the id of each node to the node. The launcher reports what it sees to the
    coordinator, a ^C or the exit of a node's process, and the coordinator shuts the app down.
    """
    flush_output()
    command = [sys.executable, "-m", "raio._py.process"]
    command += ["--coordinator_endpoint", coordinator.endpoint]
    if fast_forward:
        command.append("--fast_forward")
    children, reported, interrupt = {}, set(), None
    try:
        for node_id, node in nodes.items():
            if isinstance(node, RustNode):
                children[node_id] = node.start(node_id, coordinator.endpoint)
            else:
                children[node_id] = subprocess.Popen([*command, "--node_id", str(node_id)])
        try:
            _report_exits(children, coordinator, reported)
        except KeyboardInterrupt as error:
            # On ^C every node runs its shutdown method at once and then ends by itself.
            interrupt = error
            coordinator.shutdown_now()
            _report_exits(children, coordinator, reported)
        # Raises the reason if the coordinator refused the graph.
        failed, silent = coordinator.join()
        # The coordinator has given up on the nodes that went silent, so nothing waits for them.
        end_processes([children[node_id] for node_id in silent])
        statuses = {node_id: child.wait() for node_id, child in children.items()}
    except BaseException:
        end_processes(children.values())  # A second ^C, or any other error, ends the nodes.
        raise
    if interrupt is not None:
        raise interrupt
    # A node may still fail once the coordinator has seen it end.
    failed += [node_id for node_id, status in statuses.items() if status != 0]
    if failed:
        node_id = failed[0]
        outcome = f"exited with status {statuses[node_id]}"
        if node_id in silent:
            outcome = "stopped responding"
        raise RuntimeError(f"node {node_id} ({type(nodes[node_id]).__name__}) {outcome}")


def _report_exits(children, coordinator, reported):
    """Report the exit of each node's process to the coordinator, until it has ended the app."""
    while not coordinator.finished():
        for node_id, child in children.items():
            if node_id not in reported and child.poll() is not None:
                coordinator.node_exited(node_id, child.returncode == 0)
                reported.add(node_id)
        time.sleep(0.01)  # The launcher only looks on, so it need not see an exit at once.
