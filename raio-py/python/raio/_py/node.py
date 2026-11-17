"""Nodes and the tasks declared on them."""

from __future__ import annotations

import inspect

from . import states
from .ports import InputPort, _Port
from .utils import declared


def task(method):
    """Run an async method as a task when the node starts."""
    if not inspect.iscoroutinefunction(method):
        raise TypeError("@task needs an async method")
    method._raio_task = True
    return method


class _NodeMeta(type):
    def __call__(cls, *args, **kwargs):
        node = cls.__new__(cls)
        node._ports = []  # every port of the node, in the order the class declares them
        # Each instance gets its own port for every port declared on the class.
        for name, declaration in declared(cls, lambda value: isinstance(value, _Port)):
            port = type(declaration)()
            port._node, port._name, port._index = node, name, len(node._ports)
            node._ports.append(port)
            setattr(node, name, port)
        # The node's own process gets the class with its attributes sorted by name, so the
        # order in which the class declares its tasks is kept on the node.
        tasks = declared(cls, lambda value: hasattr(value, "_raio_task"))
        node._tasks = [name for name, _ in tasks]
        node.__init__(*args, **kwargs)
        return node


class Node(metaclass=_NodeMeta):
    """A unit of computation with its own process, ports, tasks, and logical clock."""


def _kinds(node):
    """Name the kind of each port of a node, in order, for raio-rs."""
    return ["input" if isinstance(port, InputPort) else "output" for port in node._ports]


def _start(node):
    """Make the node the one this process runs and create its tasks in their declared order."""
    states.running = node
    return [getattr(node, name)() for name in node._tasks]
