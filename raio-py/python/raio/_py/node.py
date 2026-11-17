"""Nodes and the tasks and reactions declared on them."""

from __future__ import annotations

import inspect

from .._rs import core
from . import states
from .ports import InputPort, _Port
from .utils import declared


def task(method):
    """Run an async method as a task when the node starts."""
    if not inspect.iscoroutinefunction(method):
        raise TypeError("@task needs an async method")
    method._raio_triggers = None  # a task has no triggers: it runs once, from the start
    return method


def reaction(*triggers):
    """Run a method once per tag at which any trigger has a value."""
    if not triggers or not all(isinstance(trigger, InputPort) for trigger in triggers):
        raise TypeError("@reaction needs the input ports that trigger it")

    def mark(method):
        method._raio_triggers = triggers
        return method

    return mark


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
        # order in which the class declares its tasks and reactions is kept on the node.
        tasks = declared(cls, lambda value: hasattr(value, "_raio_triggers"))
        node._tasks = [name for name, _ in tasks]
        node.__init__(*args, **kwargs)
        return node


class Node(metaclass=_NodeMeta):
    """A unit of computation with its own process, ports, tasks, and logical clock."""


def _kinds(node):
    """Name the kind of each port of a node, in order, for raio-rs."""
    return ["input" if isinstance(port, InputPort) else "output" for port in node._ports]


async def _arrives(port):
    """Wait for the next value of a port; return False if the port closes instead."""
    try:
        await port
    except core.Closed:
        return False
    return True


async def _react(node, method, triggers):
    """Call a reaction at each tag where a trigger has a value, until every trigger closes."""
    # The triggers are the declarations on the class; the node's own ports have their names.
    waiting = [getattr(node, trigger._name) for trigger in triggers]
    while waiting:
        arrived = await core.select(*map(_arrives, waiting))
        # A port that closed is waited for no longer; one that is absent has not finished.
        waiting = [port for index, port in enumerate(waiting) if arrived.get(index, True)]
        if any(arrived.values()):
            result = method()
            if inspect.isawaitable(result):
                await result


def _start(node):
    """Make the node the one this process runs and create its tasks in their declared order."""
    states.running = node
    coroutines = []
    for name in node._tasks:
        method = getattr(node, name)
        triggers = method._raio_triggers
        coroutines.append(method() if triggers is None else _react(node, method, triggers))
    return coroutines
