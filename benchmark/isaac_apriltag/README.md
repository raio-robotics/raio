# Native Raio and Isaac ROS AprilTag on an A40

This compares the published Isaac ROS 4.6 AprilTag node graph with a native Raio detection path on the same GPU. NVIDIA's source graph resizes a slice of the r2b_storage recording, buffers ten HD frames, publishes images and calibration, runs cuAprilTags, and monitors AprilTagDetectionArray. The Raio implementation uses the same static CUDA archive included with the installed NITROS package. Its own detector library links CUDA and the C++ runtime. The detector backend, intrinsics, 0.22 m tag size, tile size 4, tag36h11 family and maximum of 64 detections match the stock node.

## Measured work

A separate observer captures the exact ten preprocessed frames, calibration messages and per-frame ROS detections in a short 30 Hz playback before the timed blocks in each stock launch. It exits before steady measurements. The Raio launcher checks each image digest and preloads the immutable images into GPU memory. Its source publishes a CUDA IPC frame descriptor and a separate timestamped calibration value on native ports. The native detector reads matching values at the same logical time. It opens the GPU allocation once, uses the source's image pitch and intrinsics, synchronizes CUDA and sends IDs, centers, corners and poses to a separate Raio monitor. The exporting process retains ownership until every child exits. Recorded data and static calibration are specific to this bounded playback test.

Both variants run one warm-up block followed by three measured five-second blocks at each target rate. Each retained ROS block uses the stock C++ composable graph, NITROS image delivery, stock playback and stock detection monitor. The controller exports timestamps *after* monitoring stops. The stock clock starts after publishing the image and calibration group. The Raio clock starts after setting those two ports, before Raio flushes them to the downstream process at the end of its logical step. End time is monitor receipt. This measures the two implemented detection paths including their runtime overhead. The stock node also publishes TF; the Raio graph streams the detection results to one consumer. ROS bag IO, image resizing, GPU preload and initialization are outside the measured interval.

The analysis checks every native output against the stock result for the same input stamp. Tolerances are 0.05 px maximum corner-coordinate difference, 1 mm translation norm and 0.1 degrees rotation difference using the absolute quaternion dot product. The fixture contains moving views of a single detected tag. The reported p95 is nearest rank within each retained block, then the median across blocks. The stock monitor uses independent sequence counters by default. Received and sent timestamps are paired in order only in complete blocks. Any loss suppresses the across-block p95 for that runtime/rate. Report counts and achieved rates even when latency cannot be paired. This fixed-rate test does not establish maximum sustainable throughput or live-camera latency.

## Reproduction

From the checkout root, stage the scripts in a fresh directory on an x86-64 GPU host with Docker's NVIDIA runtime.

```sh
trial=$(mktemp -d)
cp benchmark/isaac_apriltag/*.{py,cpp,sh} "$trial/"
docker run --rm -it --gpus device=0 --shm-size=2g -v "$trial:/trial" -w /trial ros:jazzy-ros-base bash
```

Inside the container, install the detector and download the two files from the NVIDIA r2b dataset (approximately 3.1 GB).

```sh
set -e
bash provision.sh
python3 -m pip install --target /trial/deps --no-deps raio-py==0.2.0 cloudpickle==3.1.3 msgpack==1.2.4
assets=/trial/assets/datasets/r2b_dataset/r2b_storage
mkdir -p "$assets"
base=https://api.ngc.nvidia.com/v2/resources/nvidia/isaac/r2bdataset2023/versions/2/files/r2b_storage
for file in metadata.yaml r2b_storage_0.db3; do
    curl -fL --retry 3 "$base/$file" -o "$assets/$file"
done
echo "19fb481a704aed79390e6878d509d9ba4168895dce62ee91ad476e71c5ec8584  $assets/r2b_storage_0.db3" | sha256sum -c -
bash build-native.sh
for hz in 30 60 120 300; do bash matched.sh "$hz"; done
python3 report.py 30 60 120 300
```

The output directory contains per-rate fixtures, raw timestamps, stock reports and the combined `summary.json`. Each rate requires a fresh output path.

The ROS launcher uses the installed package's benchmark file and appends controller-only code for fixture capture and post-measurement export. The original source and public data are documented in [NVIDIA's AprilTag benchmark](https://github.com/NVIDIA-ISAAC-ROS/isaac_ros_benchmark/blob/release-4.6/benchmarks/isaac_ros_apriltag_benchmark/scripts/isaac_ros_apriltag_node.py) and the [stock CUDA node implementation](https://github.com/NVIDIA-ISAAC-ROS/isaac_ros_apriltag/blob/release-4.6/isaac_ros_apriltag/src/apriltag_node.cpp).
