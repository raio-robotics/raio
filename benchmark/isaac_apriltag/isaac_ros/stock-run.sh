#!/bin/bash
set -e
script_dir=$(cd "$(dirname "$0")" && pwd)
distro=${ROS_DISTRO:-jazzy}
source "/opt/ros/$distro/setup.bash"
arch=$(gcc -print-multiarch)
export LD_LIBRARY_PATH="/usr/local/cuda/lib64:/opt/nvidia/vpi4/lib/$arch:${LD_LIBRARY_PATH:-}"
export ISAAC_ROS_WS=/trial
export ROS2_BENCHMARK_OVERRIDE_PRE_STOP_MONITORING_WAIT_TIME_SEC=0.5
export ROS2_BENCHMARK_OVERRIDE_ASSETS_ROOT=/trial/assets
export ROS2_BENCHMARK_OVERRIDE_PUBLISHER_UPPER_FREQUENCY="$1" ROS2_BENCHMARK_OVERRIDE_PUBLISHER_LOWER_FREQUENCY="$1"
iterations=4
# Lyrical runs a dedicated, unmeasured fixed-rate warm-up in ros2_benchmark.
if [[ "$distro" == lyrical ]]; then iterations=3; fi
export ROS2_BENCHMARK_OVERRIDE_TEST_ITERATIONS=$iterations ROS2_BENCHMARK_OVERRIDE_EXPORT_MONITOR_RAW_DATA=true
export ROS2_BENCHMARK_OVERRIDE_LOG_FOLDER="$PWD/$2" ROS2_BENCHMARK_OVERRIDE_ENABLE_RESOURCE_PROFILER=false
mkdir -p "$ROS2_BENCHMARK_OVERRIDE_LOG_FOLDER"
script=$(find "/opt/ros/$distro" -path '*/isaac_ros_apriltag_benchmark/scripts/isaac_ros_apriltag_node.py' -print -quit)
test -n "$script"
sha256sum "$script"
export PYTHONPATH="$script_dir:${PYTHONPATH:-}"
instrumented="$ROS2_BENCHMARK_OVERRIDE_LOG_FOLDER/stock_with_trace.py"
cp "$script" "$instrumented"
cat >> "$instrumented" <<'PYTHON'

from record_timings import configure
configure(TestIsaacROSAprilTagNode)
PYTHON
if [[ "$distro" == lyrical && -z ${IMAGE_PUBLICATIONS:-} ]]; then
    probe="$ROS2_BENCHMARK_OVERRIDE_LOG_FOLDER/publication.so"
    g++ -std=c++17 -O2 -fPIC -shared "$script_dir/publication.cpp" -ldl -o "$probe"
    export IMAGE_PUBLICATIONS="$ROS2_BENCHMARK_OVERRIDE_LOG_FOLDER/publications.csv"
    export LD_PRELOAD="$probe${LD_PRELOAD:+:$LD_PRELOAD}"
fi
exec launch_test "$instrumented"
