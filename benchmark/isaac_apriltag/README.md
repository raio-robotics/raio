# AprilTag benchmark

Replay the same ten 1280 × 720 frames and camera calibration through the native Raio and Isaac ROS AprilTag pipelines. Both use NVIDIA's cuAprilTags detector. The paired benchmark measures detection latency and counts delivery on both the detection and pose outputs. [Orin and A40 results, stock reports, and measurement method](../results/isaac_apriltag/README.md)

## Reproduction

### Run on Jetson AGX Orin

The reported run used a 64 GB AGX Orin Developer Kit, JetPack 7.2.2 in MAXN mode, Isaac ROS 5.1 on ROS 2 Lyrical, and Raio 0.2.0. Use a GPU-enabled JetPack container with CUDA exposed at `/usr/local/cuda`, a writable `/trial`, and the checkout accessible inside the container. Configure the Isaac ROS 5.1 and Lyrical package repositories and install these dependencies. [NVIDIA setup](https://nvidia-isaac-ros.github.io/getting_started/index.html)

```sh
apt-get install -y ros-lyrical-isaac-ros-apriltag-benchmark \
    ros-lyrical-launch-testing-ros ros-lyrical-rosbag2-storage-sqlite3 \
    libeigen3-dev g++ curl python3-pip python3-numpy python3-pytest
```

Prepare the recording (about 3.1 GB) and the Raio Python package once per environment.

```sh
python3 -m pip install --target /trial/deps raio-py==0.2.0
assets=/trial/assets/datasets/r2b_dataset/r2b_storage
mkdir -p "$assets"
base=https://api.ngc.nvidia.com/v2/resources/nvidia/isaac/r2bdataset2023/versions/2/files/r2b_storage
for file in metadata.yaml r2b_storage_0.db3; do
    curl -fL --retry 3 "$base/$file" -o "$assets/$file"
done
echo "19fb481a704aed79390e6878d509d9ba4168895dce62ee91ad476e71c5ec8584  $assets/r2b_storage_0.db3" | sha256sum -c -
```

From the checkout root inside the container, run the paired test at 193 fps in a fresh directory.

```bash
trial=$(mktemp -d /trial/apriltag.XXXXXX)
cp -R benchmark/isaac_apriltag/{matched.sh,raio,isaac_ros,analysis} "$trial/"
cd "$trial"
export ROS_DISTRO=lyrical OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1
bash raio/build-native.sh
bash matched.sh 193
python3 analysis/report.py 193
```

`summary.json` contains the validated counts and timings. The trial also keeps the reference frames and raw timestamps. Each launch measures three five-second playbacks per runtime. Run a second fresh trial to match the six playbacks in the results table. Replace `193` in both commands to test the other Orin rates (30, 120, 180, 191, or 240 fps).

### Run NVIDIA's unmodified benchmark

In a fresh shell in the same Lyrical container, run the installed benchmark directly for the 191 fps comparison. Omit the frequency overrides for NVIDIA's default throughput search, or set both to 240 for the other fixed-rate control.

```sh
source /opt/ros/lyrical/setup.bash
export ISAAC_ROS_WS=/trial ROS2_BENCHMARK_OVERRIDE_ASSETS_ROOT=/trial/assets
export LD_LIBRARY_PATH="/usr/local/cuda/lib64:/opt/nvidia/vpi4/lib/$(gcc -print-multiarch):${LD_LIBRARY_PATH:-}"
export ROS2_BENCHMARK_OVERRIDE_LOG_FOLDER=$(mktemp -d /trial/apriltag-control.XXXXXX)
export ROS2_BENCHMARK_OVERRIDE_PUBLISHER_LOWER_FREQUENCY=191
export ROS2_BENCHMARK_OVERRIDE_PUBLISHER_UPPER_FREQUENCY=191
launch_test /opt/ros/lyrical/share/isaac_ros_apriltag_benchmark/scripts/isaac_ros_apriltag_node.py
```

### Reproduce the A40 runs

For Isaac ROS 5.1, use an Ubuntu 24.04 GPU container with CUDA, the x86-64 Isaac ROS 5.1 and Lyrical repositories, and the same dataset and dependencies above. Run the paired test at 30, 60, 120, 180, and 240 fps.

For the earlier Isaac ROS 4.6 run, use `ros:jazzy-ros-base` with NVIDIA Docker on x86-64. From the checkout root, put the scripts in the directory mounted at `/trial`.

```bash
trial=$(mktemp -d)
cp -R benchmark/isaac_apriltag/{matched.sh,raio,isaac_ros,analysis} "$trial/"
docker run --rm -it --gpus device=0 --shm-size=2g -v "$trial:/trial" -w /trial ros:jazzy-ros-base bash
```

Inside that container, run `bash isaac_ros/provision.sh`, install `libeigen3-dev`, and prepare the recording and Raio package using the shared commands above. Set `ROS_DISTRO=jazzy`, then build and run the paired test at the five A40 rates.

## File guide

| Path | Role |
| --- | --- |
| [`matched.sh`](matched.sh) | Runs one paired trial at the requested input rate. |
| [`raio/`](raio/) | Python playback and detector nodes, the C++ cuAprilTags wrapper, and Orin GPU memory sharing. Build with `bash raio/build-native.sh`. Generate the node diagram with `python3 raio/draw_raio_graph.py apriltag-raio.svg`. |
| [`isaac_ros/`](isaac_ros/) | Launches and instruments NVIDIA's benchmark, captures the reference frames, and listens for tag poses. |
| [`analysis/`](analysis/) | Validates detections and poses, reports latency, combines trial data, and draws the delivery chart. |

The [results directory](../results/isaac_apriltag/README.md) contains the saved measurements, timing and validation details, and the Isaac ROS graph snapshot.
