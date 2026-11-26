#!/bin/bash
set -e
cd "$(dirname "$0")"
source /opt/ros/jazzy/setup.bash
export LD_LIBRARY_PATH="/usr/local/cuda/lib64:/opt/nvidia/vpi4/lib/x86_64-linux-gnu:${LD_LIBRARY_PATH:-}"
export ISAAC_ROS_WS=/trial
export ROS2_BENCHMARK_OVERRIDE_PRE_STOP_MONITORING_WAIT_TIME_SEC=0.5
export ROS2_BENCHMARK_OVERRIDE_ASSETS_ROOT=/trial/assets
export ROS2_BENCHMARK_OVERRIDE_PUBLISHER_UPPER_FREQUENCY="$1" ROS2_BENCHMARK_OVERRIDE_PUBLISHER_LOWER_FREQUENCY="$1"
export ROS2_BENCHMARK_OVERRIDE_TEST_ITERATIONS=4 ROS2_BENCHMARK_OVERRIDE_EXPORT_MONITOR_RAW_DATA=true
export ROS2_BENCHMARK_OVERRIDE_LOG_FOLDER="$PWD/$2" ROS2_BENCHMARK_OVERRIDE_ENABLE_RESOURCE_PROFILER=false
mkdir -p "$ROS2_BENCHMARK_OVERRIDE_LOG_FOLDER"
script=$(find /opt/ros/jazzy -path '*/isaac_ros_apriltag_benchmark/scripts/isaac_ros_apriltag_node.py' -print -quit)
test -n "$script"
sha256sum "$script"
export PYTHONPATH="$PWD:${PYTHONPATH:-}"
instrumented="$ROS2_BENCHMARK_OVERRIDE_LOG_FOLDER/stock_with_trace.py"
cp "$script" "$instrumented"
cat >> "$instrumented" <<'PYTHON'

from record_timings import configure
configure(TestIsaacROSAprilTagNode)
PYTHON
exec launch_test "$instrumented"
