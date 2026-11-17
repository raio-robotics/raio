"""Connecting ports and running an app: one process per node, wired by a coordinator."""

from __future__ import annotations

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

_connections = []  # (source, target, delay, serializer) of every connection made in this process


def connect(
    source: OutputPort[T],
    target: InputPort[T],
    *,
    delay: Time = Time.ZERO,
    serializer: Serializer[T] | None = None,
) -> None:
    """Send every value of an output port to an input port, shifted by a logical delay."""
    if not isinstance(source, OutputPort) or source._node is None:
        raise ValueError(f"connect needs an output port of a node instance, got {source!r}")
    if not isinstance(target, InputPort) or target._node is None:
        raise ValueError(f"connect needs an input port of a node instance, got {target!r}")
    if not isinstance(delay, Time) or delay < Time.ZERO or delay.microstep:
        raise ValueError("a connection delay is a Time of zero or more nanoseconds")
    _connections.append((source, target, delay, serializer))


def run(*nodes: Node, fast_forward: bool = False) -> None:
    """Run each node in its own process until all of them finish."""
    if states.running is not None:
        raise RuntimeError("raio.run cannot be called from inside a node")
    if not nodes:
        raise ValueError("run needs at least one node")
    positions = {}
    for node in nodes:
        if not isinstance(node, Node):
            raise TypeError(f"run takes node instances, got {node!r}")
        if id(node) in positions:
            raise ValueError(f"{type(node).__name__} was given to run twice")
        positions[id(node)] = len(positions)
    connections, serializers = _wire(positions)
    # Pickling comes first, so that a node that cannot be sent fails before anything starts.
    # Each node's process imports from the same places as this one, so the path goes with it.
    code = {}
    for index, node in enumerate(nodes):
        if not isinstance(node, RustNode):  # A Rust node is a program of its own and has no code.
            code[index] = pickle.dumps((sys.path, cloudpickle.dumps((node, serializers[index]))))
    coordinator = core.coordinator(len(nodes), connections, code)
    _launch(nodes, coordinator, fast_forward)


def _wire(positions):
    """List the connections among the nodes, and the serializers that each node's ports take.

    `positions` maps the id of each node of the app to the node's position in it.
    """
    connections = []
    serializers = [[] for _ in positions]  # (port, name, serializer) for each node's ports
    for source, target, delay, serializer in _connections:
        ends = (id(source._node) in positions, id(target._node) in positions)
        if ends == (False, False):
            continue  # A connection of another app.
        if ends != (True, True):
            raise ValueError(f"{source!r} feeds {target!r}, but only one of their nodes is run")
        source_node, target_node = positions[id(source._node)], positions[id(target._node)]
        connections.append((source_node, source._index, target_node, target._index, delay.nanos))
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

    The launcher reports what it sees to the coordinator, a ^C or the exit of a node's process,
    and the coordinator shuts the app down.
    """
    flush_output()
    command = [sys.executable, "-m", "raio._py.process"]
    command += ["--coordinator_endpoint", coordinator.endpoint]
    if fast_forward:
        command.append("--fast_forward")
    children, reported, interrupt = [], set(), None
    try:
        for index, node in enumerate(nodes):
            if isinstance(node, RustNode):
                child = node.start(index, coordinator.endpoint)
            else:
                child = subprocess.Popen([*command, "--node_id", str(index)])
            children.append(child)
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
        end_processes([children[index] for index in silent])
        statuses = [child.wait() for child in children]
    except BaseException:
        end_processes(children)  # A second ^C, or any other error, ends the nodes at once.
        raise
    if interrupt is not None:
        raise interrupt
    # A node may still fail once the coordinator has seen it end.
    failed += [index for index, status in enumerate(statuses) if status != 0]
    if failed:
        index = failed[0]
        outcome = f"exited with status {statuses[index]}"
        if index in silent:
            outcome = "stopped responding"
        raise RuntimeError(f"node {index} ({type(nodes[index]).__name__}) {outcome}")


def _report_exits(children, coordinator, reported):
    """Report the exit of each node's process to the coordinator, until it has ended the app."""
    while not coordinator.finished():
        for index, child in enumerate(children):
            if index not in reported and child.poll() is not None:
                coordinator.node_exited(index, child.returncode == 0)
                reported.add(index)
        time.sleep(0.01)  # The launcher only looks on, so it need not see an exit at once.
