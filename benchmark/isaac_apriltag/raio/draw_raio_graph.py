"""Export the paired AprilTag nodes and ports without running the detector."""

import argparse
from pathlib import Path

import raio

from pipeline import make_nodes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    nodes = make_nodes(193, 193 * 5 * 4, args.output.parent, [], [])
    raio.diagram(args.output, *nodes)


if __name__ == "__main__":
    main()
