"""A node's process: it asks the coordinator for its node and runs it until the node finishes."""

from __future__ import annotations

import argparse
import pickle
import signal
import sys
from typing import Callable, NamedTuple

import cloudpickle

from .._rs import core
from .node import _start
from .utils import buffer_output_by_line


class _Loaded(NamedTuple):
    """Everything that raio-rs needs to build a node and run it."""

    kinds: list[str]  # the kind of each port, in the order of their positions
    serializers: list[tuple]  # (port position, name, serializer) for each connection's choice
    start: Callable[[], list]  # returns the coroutines of the node's tasks
    shutdown: Callable[[], None]


def _load(code) -> _Loaded:
    """Rebuild the node from the code that the launcher left with the coordinator."""
    path, payload = pickle.loads(code)
    sys.path[:] = path  # The node's classes are imported from where the launcher found them.
    node, serializers = cloudpickle.loads(payload)
    kinds = [port._kind for port in node._ports]
    return _Loaded(kinds, serializers, lambda: _start(node), node.shutdown)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node_id", type=int, required=True)
    parser.add_argument("--coordinator_endpoint", required=True)
    parser.add_argument("--fast_forward", action="store_true")
    args = parser.parse_args()
    # ^C is left to the launcher, which shuts the nodes down through the coordinator.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    buffer_output_by_line()
    core.run_node(args.node_id, args.coordinator_endpoint, _load, args.fast_forward)


if __name__ == "__main__":
    main()
