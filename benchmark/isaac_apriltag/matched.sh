#!/bin/bash
set -e
cd "$(dirname "$0")"
source "/opt/ros/${ROS_DISTRO:-jazzy}/setup.bash"
export LD_LIBRARY_PATH="/usr/local/cuda/lib64:/opt/nvidia/vpi4/lib/$(gcc -print-multiarch):${LD_LIBRARY_PATH:-}"
rate=${1:?supply the positive integer target rate in Hz}
[[ "$rate" =~ ^[1-9][0-9]*$ ]] || exit 2
for path in "fixture-$rate" "stock-$rate" "raio-$rate"; do
    if [[ -e "$path" ]]; then echo "Output exists: $path" >&2; exit 1; fi
done
if [[ ${ROS_DISTRO:-} == lyrical && -r /sys/devices/soc0/machine ]] && grep -qi Orin /sys/devices/soc0/machine; then
    export APRILTAG_RELIABLE_CAPTURE=1
fi
export FIXTURE="$PWD/fixture-$rate"
stock="$PWD/stock-$rate"
mkdir "$stock"
if [[ "${ROS_DISTRO:-jazzy}" == lyrical ]]; then export APRILTAG_CAPTURE_CONTROL="$stock"; fi
python3 observe_tf.py "$stock" > "tf-$rate.log" 2>&1 &
listener=$!
trap 'kill "$listener" 2>/dev/null || true' EXIT
for ((i=0; i<150; i++)); do
    [[ -e "$stock/tf-ready" ]] && break
    kill -0 "$listener"
    sleep 0.1
done
test -e "$stock/tf-ready"
python3 capture.py > "capture-$rate.log" 2>&1 &
observer=$!
trap 'kill "$observer" "$listener" 2>/dev/null || true' EXIT
bash ./stock-run.sh "$rate" "stock-$rate" > "stock-$rate.log" 2>&1
touch "$stock/tf-stop"
wait "$listener"
wait "$observer"
trap - EXIT
test -s "$FIXTURE/manifest.json"
env -u AMENT_PREFIX_PATH -u ROS_DISTRO -u ROS_VERSION PYTHONPATH=/trial/deps python3 ./native.py "$PWD/raio-$rate" "$rate" "$((rate*5*4))" "$FIXTURE" > "raio-$rate.log" 2>&1
