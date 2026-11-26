#!/bin/bash
set -e
cd "$(dirname "$0")"
source /opt/ros/jazzy/setup.bash
rate=${1:?supply the positive integer target rate in Hz}
[[ "$rate" =~ ^[1-9][0-9]*$ ]] || exit 2
for path in "fixture-$rate" "stock-$rate" "raio-$rate"; do
    if [[ -e "$path" ]]; then echo "Output exists: $path" >&2; exit 1; fi
done
export FIXTURE="$PWD/fixture-$rate"
python3 capture.py > "capture-$rate.log" 2>&1 &
observer=$!
trap 'kill "$observer" 2>/dev/null || true' EXIT
bash ./stock-run.sh "$rate" "stock-$rate" > "stock-$rate.log" 2>&1
wait "$observer"
test -s "$FIXTURE/manifest.json"
env -u AMENT_PREFIX_PATH -u ROS_DISTRO -u ROS_VERSION PYTHONPATH=/trial/deps python3 ./native.py "$PWD/raio-$rate" "$rate" "$((rate*5*4))" "$FIXTURE" > "raio-$rate.log" 2>&1
