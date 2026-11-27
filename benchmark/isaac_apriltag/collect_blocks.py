"""Export verified five-second blocks from independent AprilTag launches."""

import argparse
import csv
from pathlib import Path

from report import analyze_rate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--launch", nargs=2, action="append", required=True,
                        metavar=("LABEL", "DIRECTORY"))
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    rows = []
    seen = set()
    for label, directory in args.launch:
        folder = Path(directory)
        clocks = sorted(folder.glob("stock-[0-9]*/intervals.jsonl"))
        if not clocks:
            parser.error(f"no stock timing blocks in {folder}")
        for clock in clocks:
            rate = int(clock.parent.name.removeprefix("stock-"))
            if (label, rate) in seen:
                parser.error(f"duplicate launch {label} at {rate} fps")
            seen.add((label, rate))
            data = analyze_rate(folder, rate)
            for runtime, blocks in data["trials"].items():
                transforms = data["transforms"][f"{runtime}_blocks_received"]
                for number, (block, tf) in enumerate(zip(blocks, transforms, strict=True), 1):
                    rows.append(dict(target_fps=rate, launch=label, block=number,
                                     runtime=runtime, inputs=block["sent"],
                                     offered_fps=round(block["sent_hz"], 4),
                                     detections=block["received"], transform_batches=tf,
                                     drained_detections=block.get("drained"),
                                     last_detection_after_last_input_ms=block.get("last_detection_after_last_input_ms"),
                                     post_publication_p95_ms=block.get("p95_ms") if block["sent"] == block["received"] else None,
                                     publication_to_detection_p95_ms=block.get("publication_entry_p95_ms")))
    rows.sort(key=lambda r: (r["target_fps"], r["launch"], r["block"], r["runtime"]))
    with args.output.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=rows[0], lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
