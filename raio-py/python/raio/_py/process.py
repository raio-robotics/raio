"""A node's process: it asks the coordinator for its node and runs it until the node finishes."""

from __future__ import annotations

import argparse
import pickle
import sys

import cloudpickle

from .._rs import core
from .node import _kinds, _start
from .utils import buffer_output_by_line


def _load(code):
    """Rebuild the node from its code; return what raio-rs needs to build it and start it."""
    path, payload = pickle.loads(code)
    sys.path[:] = path  # The node's classes are imported from where the launcher found them.
    node, serializers = cloudpickle.loads(payload)
    return _kinds(node), serializers, lambda: _start(node), node.shutdown


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node_id", type=int, required=True)
    parser.add_argument("--coordinator_endpoint", required=True)
    parser.add_argument("--fast_forward", action="store_true")
    args = parser.parse_args()
    buffer_output_by_line()
    core.run_node(args.node_id, args.coordinator_endpoint, _load, args.fast_forward)


if __name__ == "__main__":
    main()
