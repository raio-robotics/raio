"""Controller instrumentation. Collection happens after the stock monitor stops."""

import json
import os
import time
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
        control = os.environ.get("APRILTAG_CAPTURE_CONTROL")
        if not control:
            self.benchmark_body(30, 30.0)
            return original_loop(self)
        # Keep external image subscriptions out of Lyrical's timed buffer negotiation.
        result = original_loop(self)
        folder = Path(control)
        (folder / "capture-start").touch()
        deadline = time.monotonic() + 15
        while not (folder / "capture-ready").exists():
            if time.monotonic() > deadline:
                raise TimeoutError("fixture observer did not start")
            time.sleep(0.05)
        time.sleep(2.0)
        capture_rate = 5.0 if os.environ.get("APRILTAG_RELIABLE_CAPTURE") else 30.0
        self.benchmark_body(30, capture_rate)
        return result

    BasicPerformanceCalculator.calculate_performance = save_timings
    test_class.config.additional_fixed_publisher_rate_tests = []
    test_class.run_benchmark_looping_mode = loop_with_fixture
