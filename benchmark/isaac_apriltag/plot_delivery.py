"""Plot the fraction of replayed frames received within the benchmark window."""

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("blocks", type=Path)
    parser.add_argument("figure", type=Path)
    args = parser.parse_args()
    counts = defaultdict(list)
    with args.blocks.open() as stream:
        for row in csv.DictReader(stream):
            counts[int(row["target_fps"]), row["runtime"]].append(row)
    rates = sorted({rate for rate, _ in counts})
    fig, axes = plt.subplots(1, 2, figsize=(9.8, 4.0), sharey=True, layout="constrained")
    for axis, (field, title) in zip(axes, (("detections", "Detections"),
                                            ("transform_batches", "Transform batches"))):
        for offset, runtime, name, color in ((-0.20, "ros", "Isaac ROS 5.1", "#32678e"),
                                              (0.20, "raio", "Raio 0.2.0", "#cf7726")):
            fractions = []
            for rate in rates:
                blocks = counts[rate, runtime]
                fractions.append(sum(int(row[field]) for row in blocks) /
                                 sum(int(row["inputs"]) for row in blocks))
            bars = axis.bar([i + offset for i in range(len(rates))], fractions, 0.37,
                            label=name, color=color)
            axis.bar_label(bars, labels=[f"{100*x:.0f}%" for x in fractions], fontsize=8)
        axis.set(title=title, xlabel="Requested input (frames/s)", ylim=(0, 1.14))
        axis.set_xticks(range(len(rates)), rates)
        axis.yaxis.set_major_formatter(PercentFormatter(1))
        axis.spines[["top", "right"]].set_visible(False)
    axes[0].set_ylabel("Outputs received / input frames")
    axes[1].legend(loc="lower left")
    fig.suptitle("AprilTag replay on Jetson AGX Orin 64 GB")
    fig.savefig(args.figure, dpi=170)
    plt.close(fig)


if __name__ == "__main__":
    main()
