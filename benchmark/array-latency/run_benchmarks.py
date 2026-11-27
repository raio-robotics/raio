#!/usr/bin/env python3
import importlib
import json
import os
import sys
import time
from typing import Callable, Dict, List, Optional

import numpy as np


def run_benchmark_loop_sync(
    array_sizes: List[int],
    multiplier_value: float,
    num_iterations: int,
    call_fn: Callable[[np.ndarray, float], np.ndarray],
    warmup_fn: Optional[Callable[[np.ndarray, float], None]] = None,
    cleanup_fn: Optional[Callable[[], None]] = None,
    warmup_steps: int = 20,
) -> Dict[int, List[float]]:
    results: Dict[int, List[float]] = {}

    for size in array_sizes:
        array = np.random.rand(size).astype(np.float64)

        # Warmup
        for _ in range(warmup_steps):
            if warmup_fn is not None:
                warmup_fn(array, multiplier_value)
            else:
                try:
                    call_fn(array, multiplier_value)
                except Exception:
                    pass

        times: List[float] = []
        for i in range(num_iterations):
            start = time.perf_counter()
            call_fn(array, multiplier_value)
            elapsed = time.perf_counter() - start
            times.append(elapsed)

        results[size] = times
        avg = np.mean(times)
        std = np.std(times)
        print(f"  Size {size:>10}: {avg * 1000:.3f} ms ± {std * 1000:.3f} ms")

    if cleanup_fn is not None:
        cleanup_fn()

    return results


# float64 = 8 bytes per element; 2^1=2 (16B) to 2^20=1048576 (8MB)
ARRAY_SIZES = [2**n for n in range(1, 21)]
NUM_ITERATIONS = 20
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results")
FRAMEWORKS = ["raio", "rosia", "ray", "rclpy"]


def main():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    selected = sys.argv[1:] or FRAMEWORKS

    all_results = {}

    for name in FRAMEWORKS:
        output_path = os.path.join(RESULTS_DIR, f"{name}_results.json")
        if name in selected:
            # Imported here, since a framework may be installed only for the interpreter that runs it.
            benchmark = getattr(importlib.import_module(f"{name}_bench"), f"benchmark_{name}")
            results = benchmark(ARRAY_SIZES, num_iterations=NUM_ITERATIONS)
            all_results[name] = {str(k): v for k, v in results.items()}
            with open(output_path, "w") as f:
                json.dump(all_results[name], f, indent=2)
            print(f"  Saved to {output_path}")
        elif os.path.exists(output_path):
            # A framework that is not run now is shown with the results it saved earlier.
            with open(output_path, "r") as f:
                all_results[name] = json.load(f)

    # Plot results
    plot_results(all_results, ARRAY_SIZES, RESULTS_DIR)

    # Print summary table
    print("\n=== Summary (median latency in ms) ===")
    header = f"{'Size':>10}"
    for framework in all_results:
        header += f" | {framework:>12}"
    has_both = "raio" in all_results and "rclpy" in all_results
    if has_both:
        header += f" | {'% faster':>12}"
    print(header)
    print("-" * len(header))
    pct_values = []
    for size in ARRAY_SIZES:
        row = f"{size:>10}"
        for framework in all_results:
            row += f" | {np.median(all_results[framework][str(size)]) * 1000:>10.3f}ms"
        if has_both:
            raio_median = np.median(all_results["raio"][str(size)])
            rclpy_median = np.median(all_results["rclpy"][str(size)])
            pct_values.append((rclpy_median - raio_median) / rclpy_median * 100)
            row += f" | {pct_values[-1]:>+10.1f}%"
        print(row)

    if has_both:
        print("-" * len(header))
        print(f"On average, raio is {np.mean(pct_values):.1f}% faster than rclpy")


def format_bytes(n_bytes: float) -> str:
    if n_bytes < 1024:
        return f"{n_bytes:.0f}B"
    elif n_bytes < 1024**2:
        return f"{n_bytes / 1024:.0f}KB"
    else:
        return f"{n_bytes / 1024**2:.1f}MB"


def plot_results(all_results: dict, array_sizes: list, results_dir: str):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    bytes_per_element = 8  # float64
    data_sizes = [s * bytes_per_element for s in array_sizes]

    fig, ax = plt.subplots(figsize=(10, 6))
    for framework, data in all_results.items():
        medians = []
        q25s = []
        q75s = []
        for size in array_sizes:
            times = np.array(data[str(size)]) * 1000
            median = np.median(times)
            medians.append(median)
            q25s.append(median - np.percentile(times, 25))
            q75s.append(np.percentile(times, 75) - median)
        ax.errorbar(
            data_sizes,
            medians,
            yerr=[q25s, q75s],
            marker="o",
            capsize=4,
            label=framework,
        )
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xticks(data_sizes)
    ax.set_xticklabels([format_bytes(s) for s in data_sizes], rotation=45)
    ax.set_xlabel("Data Size (float64 array)")
    ax.set_ylabel("Median Latency (ms)")
    ax.set_title("rclpy vs Ray vs rosia vs raio: Array Multiplication Latency")
    ax.legend()
    ax.grid(True, which="both", ls="--", alpha=0.5)
    fig.tight_layout()
    path = os.path.join(results_dir, "benchmark_results.png")
    fig.savefig(path, dpi=150)
    print(f"Plot saved to {path}")


if __name__ == "__main__":
    main()
