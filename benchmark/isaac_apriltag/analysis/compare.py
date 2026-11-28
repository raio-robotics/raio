"""Validate per-frame IDs, corner coordinates, translation and sign-invariant orientation."""

import json
import math
from statistics import median

import numpy as np


def timing(starts, ends):
    assert len(starts) == len(ends) and len(starts) > 1
    latency = [(b - a) / 1e6 for a, b in zip(starts, ends)]
    assert min(latency) >= 0
    return {
        "count": len(starts),
        "p50_ms": median(latency),
        "p95_ms": sorted(latency)[math.ceil(0.95 * len(latency)) - 1],
        "sent_hz": (len(starts) - 1) * 1e9 / (starts[-1] - starts[0]),
        "received_hz": (len(ends) - 1) * 1e9 / (ends[-1] - ends[0]),
    }


def validate(directory, rows):
    seqs = json.loads((directory / "observed.json").read_text())
    sent = json.loads((directory / "sent.json").read_text())
    assert [s[0] for s in seqs] == list(range(len(sent)))
    errors = np.zeros(3)
    n_tags = 0
    for seq, stamp, _, tags in seqs:
        ref = rows[seq % len(rows)]
        assert stamp == ref["stamp"]
        expected = sorted(ref["tags"], key=lambda t: (t["id"], t["corners"]))
        actual = sorted(tags, key=lambda t: (t[0], t[1]))
        assert [a[0] for a in actual] == [b["id"] for b in expected]
        for (tag, corners, xyz, xyzw, center), b in zip(actual, expected):
            q1, q2 = np.array(xyzw), np.array(b["xyzw"])
            assert (
                np.all(np.isfinite(center))
                and np.linalg.norm(q1) > 0.9
                and np.linalg.norm(q2) > 0.9
            )
            cos = min(1.0, abs(q1 @ q2) / (np.linalg.norm(q1) * np.linalg.norm(q2)))
            err = [
                np.max(np.abs(np.reshape(corners, (4, 2)) - b["corners"])),
                np.linalg.norm(np.array(xyz) - b["xyz"]),
                math.degrees(2 * math.acos(cos)),
            ]
            errors = np.maximum(errors, err)
            n_tags += 1
    assert n_tags and np.all(errors <= [0.05, 0.001, 0.1]), (n_tags, errors)
    return dict(
        **timing(sent, [s[2] for s in seqs]), tags=n_tags, max_errors=errors.tolist()
    )


def validate_transforms(stock_dir, native_dir, rows, stock_blocks, starts, block_size):
    """Check ROS frame semantics and native transform delivery independently of detections."""
    stock = json.loads((stock_dir / "transforms.json").read_text())
    native = json.loads((native_dir / "transforms.json").read_text())
    detections = json.loads((native_dir / "observed.json").read_text())
    assert len(native) == len(detections) == len(starts)
    reference = {r["stamp"]: r for r in rows}
    seen_stamps = set()
    allows_empty = any(not r["tags"] for r in rows)
    for _, transforms in stock:
        if not transforms:
            # An empty ROS TFMessage has no header to match to an individual image.
            assert allows_empty, "unexpected empty stock transform batch"
            continue
        stamp = transforms[0][0]
        frame = reference[stamp]
        expected = sorted(
            (stamp, frame["frame_id"], f"tag36h11:{t['id']}", t["xyz"], t["xyzw"])
            for t in frame["tags"]
        )
        assert [tuple(t) for t in sorted(transforms)] == expected
        seen_stamps.add(stamp)
    assert seen_stamps == {s for s, r in reference.items() if r["tags"]}
    for i, ((seq, receipt, transforms), (dseq, stamp, _, tags)) in enumerate(
        zip(native, detections)
    ):
        assert seq == dseq == i and receipt >= starts[i]
        row = reference[stamp]
        expected = [
            (stamp, row["frame_id"], f"tag36h11:{tag_id}", xyz, xyzw)
            for tag_id, _, xyz, xyzw, _ in tags
        ]
        assert [tuple(t) for t in transforms] == expected
    ros_counts, native_counts = [], []
    for i, (published, _) in enumerate(stock_blocks, start=1):
        first, last = published[0][1], published[-1][1]
        ros_counts.append(
            sum(first <= clock <= last + 500_000_000 for clock, _ in stock)
        )
        block = native[i * block_size : (i + 1) * block_size]
        deadline = starts[(i + 1) * block_size - 1] + 500_000_000
        native_counts.append(sum(receipt <= deadline for _, receipt, _ in block))
    assert all(0 < n <= block_size for n in ros_counts)
    return {
        "ros_blocks_received": ros_counts,
        "raio_blocks_received": native_counts,
        "ros_messages_checked_including_fixture_and_warmup": len(stock),
        "raio_messages_checked_including_warmup": len(native),
    }
