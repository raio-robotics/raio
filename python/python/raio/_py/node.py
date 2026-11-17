"""Nodes and the tasks and reactions declared on them."""

from __future__ import annotations

import inspect

from .._rs import core
from . import states
from .ports import _Input, _Port
from .utils import declared


def task(method):
    """Run an async method as a task when the node starts."""
    if not inspect.iscoroutinefunction(method):
        raise TypeError("@task needs an async method")
    method._raio_triggers = None  # a task has no triggers: it runs once, from the start
    return method


def reaction(*triggers):
    """Run a method once per timestamp at which one of its triggers has a value.

    A trigger is an input port or an action of the node.
    """
    if not triggers or not all(isinstance(trigger, _Input) for trigger in triggers):
        raise TypeError("@reaction needs the input ports or actions that trigger it")

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

    def shutdown(self) -> None:
        """Run once when the node shuts down, with the app or on its own; override to use it.

        When the app shuts down, this runs at the app's shutdown timestamp, where nothing else runs.
        When the node finishes on its own, this runs at the node's last timestamp.
        """


class RustNode(Node):
    """A node written in Rust: its ports, and how to start the program that runs it.

    The class declares the node's ports in the order in which the Rust node builds them.
    """

    def start(self, node_id: int, coordinator_endpoint: str):
        """Start the program that runs this node of the app and return its `subprocess.Popen`.

        A program that calls raio-rs's `run_node` takes the two values as the arguments
        `--node_id` and `--coordinator_endpoint`, after its own.
        """
        raise NotImplementedError(f"{type(self).__name__} needs a start method")


async def _react(node, method, triggers):
    """Call a reaction at each timestamp where a trigger has a value, until every trigger closes."""
    # The triggers are the declarations on the class; the node's own ports have their names.
    positions = [getattr(node, trigger._name)._use() for trigger in triggers]
    while await core.next_event(positions):
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
