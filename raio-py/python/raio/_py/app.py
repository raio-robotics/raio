"""Connecting ports and running an app: one process per node, wired by a coordinator."""

from __future__ import annotations

import pickle
import queue
import subprocess
import sys
import threading
from typing import Protocol, TypeVar

import cloudpickle

from .._rs import core
from .._rs.core import Time
from . import states
from .node import Node
from .ports import InputPort, OutputPort
from .utils import end_processes, flush_output

T = TypeVar("T")

_connections = []  # (source, target, delay, serializer) of every connection made in this process


class Serializer(Protocol[T]):
    """Converts the values of one connection to and from bytes."""

    def serialize(self, value: T) -> bytes: ...

    def deserialize(self, data: bytes) -> T: ...


def connect(source, target, *, delay: Time = Time.ZERO, serializer: Serializer | None = None):
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
    connections = []
    serializers = [[] for _ in nodes]  # (port, name, serializer) for each node's ports
    for source, target, delay, serializer in _connections:
        ends = (id(source._node) in positions, id(target._node) in positions)
        if ends == (False, False):
            continue  # A connection of another app.
        if ends != (True, True):
            raise ValueError(f"{source!r} feeds {target!r}, but only one of their nodes is run")
        source_node, target_node = positions[id(source._node)], positions[id(target._node)]
        connections.append((source_node, source._index, target_node, target._index, delay.nanos))
        if serializer is not None:
            # Both ends of the connection know the serializer by the name of its class.
            name = f"{type(serializer).__module__}.{type(serializer).__qualname__}"
            serializers[source_node].append((source._index, name, serializer))
            serializers[target_node].append((target._index, name, serializer))
    # Pickling comes first, so that a node that cannot be sent fails before anything starts.
    # Each node's process imports from the same places as this one, so the path goes with it.
    code = {}
    for index, node in enumerate(nodes):
        code[index] = pickle.dumps((sys.path, cloudpickle.dumps((node, serializers[index]))))
    coordinator = core.coordinator(len(nodes), connections, code)
    _launch(nodes, coordinator, fast_forward)


def _launch(nodes, coordinator, fast_forward):
    """Start one process per node, which asks the coordinator for its code, and wait for all."""
    flush_output()
    command = [sys.executable, "-m", "raio._py.process"]
    command += ["--coordinator_endpoint", coordinator.endpoint]
    if fast_forward:
        command.append("--fast_forward")
    children = []
    finished = queue.SimpleQueue()

    def wait(index, child):
        finished.put((index, child.wait()))

    failed = None
    try:
        for index in range(len(nodes)):
            child = subprocess.Popen([*command, "--node_id", str(index)])
            children.append(child)
            threading.Thread(target=wait, args=(index, child), daemon=True).start()
        for _ in children:
            index, status = finished.get()
            if status != 0:
                failed = (index, status)
                break
    except BaseException:
        end_processes(children)
        raise
    if failed is None:
        coordinator.join()
        return
    end_processes(children)
    if coordinator.finished():
        coordinator.join()  # Raises the reason if the coordinator refused the graph.
    index, status = failed
    raise RuntimeError(f"node {index} ({type(nodes[index]).__name__}) exited with status {status}")
