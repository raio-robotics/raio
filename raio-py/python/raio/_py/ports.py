"""The ports of a node: inputs, actions, and outputs."""

from __future__ import annotations

from typing import Generic, TypeVar

from .._rs import core
from .._rs.core import Time
from . import states

T = TypeVar("T")


class _Port(Generic[T]):
    """A port declared on a node class; each node instance gets its own copy."""

    def __init__(self) -> None:
        self._node = None  # None for the declaration; a copy has its node
        self._name = None
        self._index = None  # the copy's position among the ports of its node

    def __set_name__(self, owner, name):
        self._name = name

    def __repr__(self):
        owner = "<declaration>" if self._node is None else type(self._node).__name__
        return f"{owner}.{self._name}"

    def _use(self):
        """Return the port's position, which only its own running node can use."""
        if self._node is None:
            raise RuntimeError(f"{self!r} is a declaration; use the port of a node instance")
        if self._node is not states.running:
            raise RuntimeError(f"{self!r} can be used only by its own node, while that node runs")
        return self._index


class _Input(_Port[T]):
    """What a node's tasks read values from: an input port or an action."""

    @property
    def value(self) -> T | None:
        """The latest value, or None before the first one."""
        return core.input_value(self._use())

    @property
    def time(self) -> Time:
        """The timestamp of the latest value, or NEVER before the first one."""
        return core.input_time(self._use())

    @property
    def present(self) -> bool:
        """True if a value arrived at the current timestamp."""
        return core.input_present(self._use())

    @property
    def closed(self) -> bool:
        """True if no more values will arrive."""
        return core.input_closed(self._use())

    def __await__(self):
        """Wait for the next value after the current timestamp."""
        return core.input_wait(self._use(), repr(self), iterating=False)

    def __aiter__(self):
        """Iterate over the values until the port closes."""
        return self

    def __anext__(self):
        return core.input_wait(self._use(), repr(self), iterating=True)


class InputPort(_Input[T]):
    """Receives the values of one connected output port."""

    _kind = "input"  # the name by which raio-rs builds the port


class Action(_Input[T]):
    """Receives values sent from any thread as physical events; it takes no connection."""

    _kind = "action"

    def send(self, value: T) -> None:
        """Deliver a value at a fresh physical timestamp."""
        core.action_send(self._use(), repr(self), value)

    def close(self) -> None:
        """End the action after the values already sent."""
        core.action_close(self._use())


class OutputPort(_Port[T]):
    """Sends at most one value per timestamp to every connected input port."""

    _kind = "output"

    def set(self, value: T) -> None:
        """Set the value sent when the current timestamp completes."""
        core.output_set(self._use(), value)

    def reset(self) -> None:
        """Clear the value set at the current timestamp."""
        core.output_reset(self._use())
