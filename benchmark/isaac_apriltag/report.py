"""Read stock post-run clock samples and the ten-frame, per-launch correctness fixture."""

import json
import sys
from pathlib import Path
from statistics import median

from compare import timing, validate, validate_transforms


def analyze_rate(root, rate):
    manifest = json.loads((root / f"fixture-{rate}/manifest.json").read_text())
    assert (
        len(manifest)
        == len({r["stamp"] for r in manifest})
        == len({r["sha256"] for r in manifest})
        == 10
    )
    assert all((r["width"], r["height"]) == (1280, 720) for r in manifest)
    expected = rate * 5
    records = [
        json.loads(l)
        for l in (root / f"stock-{rate}/intervals.jsonl").read_text().splitlines()
    ]
    assert len(records) == 5, len(records)
    records = records[1:]  # Auxiliary stock fixture playback before the warm-up block.
    stock = []
    for starts, ends in records[1:]:
        assert len(starts) == expected and 0 < len(ends) <= len(starts)
        a = [t for _, t in starts]
        b = [t for _, t in ends]
        assert a == sorted(a) and b == sorted(b)
        result = {
            "sent": len(a),
            "received": len(b),
            "sent_hz": (len(a) - 1) * 1e9 / (a[-1] - a[0]),
            "received_hz": (len(b) - 1) * 1e9 / (b[-1] - b[0]),
        }
        if len(a) == len(b):
            result.update(timing(a, b))
        stock.append(result)
    directory = root / f"raio-{rate}"
    correctness = validate(directory, manifest)
    starts = json.loads((directory / "sent.json").read_text())
    outputs = json.loads((directory / "observed.json").read_text())
    assert len(starts) == 4 * expected
    native = []
    for i in range(1, 4):
        a = starts[i * expected : (i + 1) * expected]
        b = [r[2] for r in outputs[i * expected : (i + 1) * expected]]
        assert outputs[i * expected - 1][2] < a[0], "previous block still running"
        result = timing(a, b)
        on_time = [v for v in b if v <= a[-1] + 500_000_000]
        result.update(sent=len(a), received=len(on_time), drained=len(b))
        if len(on_time) >= 2:
            result["received_hz"] = (
                (len(on_time) - 1) * 1e9 / (on_time[-1] - on_time[0])
            )
        else:
            result["received_hz"] = 0.0
        native.append(result)
    summary = {}
    for name, trials in (("ros", stock), ("raio", native)):
        vals = {k: median(r[k] for r in trials) for k in ("received_hz", "sent_hz")}
        vals["p95_ms"] = (
            median(r["p95_ms"] for r in trials)
            if all("p95_ms" in r and r["sent"] == r["received"] for r in trials)
            else None
        )
        vals["p50_ms"] = (
            median(r["p50_ms"] for r in trials) if vals["p95_ms"] is not None else None
        )
        vals["received"] = sum(r["received"] for r in trials)
        vals["sent"] = sum(r["sent"] for r in trials)
        summary[name] = vals
    result = {
        "summary": summary,
        "trials": {"ros": stock, "raio": native},
        "tags_verified": correctness["tags"],
        "max_errors": correctness["max_errors"],
        "reference_ids": sorted({t["id"] for r in manifest for t in r["tags"]}),
    }

    if "frame_id" in manifest[0]:  # Earlier detection-only runs have no transform logs.
        result["transforms"] = validate_transforms(
            root / f"stock-{rate}", directory, manifest, records[1:], starts, expected
        )
    return result


if __name__ == "__main__":
    root = Path(__file__).resolve().parent
    rates = [int(r) for r in sys.argv[1:]]
    data = {r: analyze_rate(root, r) for r in rates}
    (root / "summary.json").write_text(json.dumps(data, indent=2) + "\n")
    print(json.dumps({r: v["summary"] for r, v in data.items()}, indent=2))
