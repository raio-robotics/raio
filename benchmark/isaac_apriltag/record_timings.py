"""Controller instrumentation. Collection happens after the stock monitor stops."""

import json
import os
from pathlib import Path

from ros2_benchmark.basic_performance_calculator import BasicPerformanceCalculator


def configure(test_class):
    original_calc = BasicPerformanceCalculator.calculate_performance
    original_loop = test_class.run_benchmark_looping_mode

    def save_timings(self, start_timestamps_ns, end_timestamps_ns):
        result = original_calc(self, start_timestamps_ns, end_timestamps_ns)
        directory = Path(os.environ["ROS2_BENCHMARK_OVERRIDE_LOG_FOLDER"])
        with (directory / "intervals.jsonl").open("a") as output:
            samples = [
                list(start_timestamps_ns.items()),
                list(end_timestamps_ns.items()),
            ]
            output.write(json.dumps(samples) + "\n")
        return result

    def loop_with_fixture(self):
        self.benchmark_body(30, 30.0)
        return original_loop(self)

    BasicPerformanceCalculator.calculate_performance = save_timings
    test_class.config.additional_fixed_publisher_rate_tests = []
    test_class.run_benchmark_looping_mode = loop_with_fixture
