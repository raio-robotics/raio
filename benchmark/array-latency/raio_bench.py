import json
import os
import time
from typing import Dict, List, Tuple

import numpy as np

import raio
from raio import InputPort, Node, OutputPort, Time, reaction, task

MICROSTEP = Time(0, 1)


class Multiplier(Node):
    input_data = InputPort[Tuple[np.ndarray, float]]()
    output_result = OutputPort[np.ndarray]()

    @reaction(input_data)
    def multiply(self):
        array, multiplier = self.input_data.value
        self.output_result.set(array * multiplier)


class Sender(Node):
    result_in = InputPort[np.ndarray]()
    data_out = OutputPort[Tuple[np.ndarray, float]]()

    def __init__(
        self,
        array_sizes: List[int],
        multiplier_value: float,
        num_iterations: int,
        results_path: str,
        warmup_steps: int = 20,
    ):
        self.array_sizes = list(array_sizes)
        self.multiplier_value = multiplier_value
        self.num_iterations = num_iterations
        self.results_path = results_path
        self.warmup_steps = warmup_steps

    @task
    async def measure(self):
        all_results: Dict[int, List[float]] = {}
        # Timestamp 0 completes at startup, so the first array is sent after it.
        await raio.wait(MICROSTEP)
        for size in self.array_sizes:
            array = np.random.rand(size).astype(np.float64)

            for _ in range(self.warmup_steps):
                await self.round_trip(array)

            times = [await self.round_trip(array) for _ in range(self.num_iterations)]

            all_results[size] = times
            avg = np.mean(times) * 1000
            std = np.std(times) * 1000
            print(f"  Size {size:>10}: {avg:.3f} ms ± {std:.3f} ms")

        # The sender runs in a process of its own, so the results reach the launcher as a file.
        with open(self.results_path, "w") as f:
            json.dump({str(k): v for k, v in all_results.items()}, f, indent=2)

    async def round_trip(self, array: np.ndarray) -> float:
        start = time.perf_counter()
        self.data_out.set((array, self.multiplier_value))
        await self.result_in
        return time.perf_counter() - start


def benchmark_raio(
    array_sizes: List[int], multiplier_value: float = 2.0, num_iterations: int = 10
) -> Dict[int, List[float]]:
    print("\n=== Benchmarking raio ===")

    from run_benchmarks import RESULTS_DIR

    os.makedirs(RESULTS_DIR, exist_ok=True)
    results_path = os.path.join(RESULTS_DIR, "raio_results.json")

    sender = Sender(array_sizes, multiplier_value, num_iterations, results_path)
    multiplier = Multiplier()

    raio.connect(sender.data_out, multiplier.input_data)
    # A loop of connections needs a delay, and the sender starts the loop before it has passed.
    raio.connect(multiplier.output_result, sender.result_in, delay=MICROSTEP)

    raio.run(sender, multiplier)

    with open(results_path, "r") as f:
        raw = json.load(f)
    return {int(k): v for k, v in raw.items()}
