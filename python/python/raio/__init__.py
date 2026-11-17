"""raio: deterministic realtime nodes on async/await.

A node declares its ports on its class and its work as `@task` and `@reaction` methods.
`raio.connect` joins an output port to an input port, and `raio.run` runs each node in its own
process until all of them finish. `raio.diagram` draws the nodes and their connections.
"""

from ._py.app import connect, diagram, run
from ._py.combinators import timeout
from ._py.node import Node, RustNode, reaction, task
from ._py.ports import Action, InputPort, OutputPort
from ._py.serializers import MessagePack, Serializer
from ._rs.core import (
    HOUR,
    MINUTE,
    MS,
    NS,
    SECOND,
    US,
    Closed,
    S,
    Task,
    Time,
    gather,
    is_physical,
    now,
    physical_now,
    request_shutdown,
    request_shutdown_after,
    select,
    spawn,
    wait,
)

__all__ = [
    "Action", "Closed", "InputPort", "MessagePack", "Node", "OutputPort", "RustNode", "Serializer",
    "Task", "Time",
    "NS", "US", "MS", "S", "SECOND", "MINUTE", "HOUR",
    "connect", "diagram", "gather", "is_physical", "now", "physical_now", "reaction",
    "request_shutdown", "request_shutdown_after", "run", "select", "spawn", "task", "timeout", "wait",
]  # fmt: skip
