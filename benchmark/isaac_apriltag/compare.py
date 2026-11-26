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
