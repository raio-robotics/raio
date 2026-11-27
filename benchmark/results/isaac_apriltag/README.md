# AprilTag benchmark results

[Reproduction and file guide](../../isaac_apriltag/README.md)

## Jetson AGX Orin 64 GB, Isaac ROS 5.1

JetPack 7.2.2, MAXN, Raio 0.2.0. Each rate has two independent launches, each with three five-second measured blocks. A probe clocks entry into the stock image-and-calibration publication. The Raio clock precedes the two port writes. Both clocks stop at detection receipt. P95 is the median of the block p95s in each launch, then the median of the two launches. A dash indicates incomplete delivery in at least one block.

Detection and transform receivers recorded identical counts in every block. Each delivery column gives the count for **each stream**, with a 0.5 s grace period after the burst.

| Target fps | Frames per runtime | Isaac ROS delivered | Raio delivered | Isaac ROS p95 (ms) | Raio p95 (ms) |
|---:|---:|---:|---:|---:|---:|
| 30 | 900 | 900 (100.0%) | 900 (100.0%) | 17.78 | 13.53 |
| 120 | 3,600 | 3,557 (98.8%) | 3,600 (100.0%) | — | 5.71 |
| 180 | 5,400 | 3,635 (67.3%) | 5,400 (100.0%) | — | 46.55 |
| 191 | 5,730 | 3,482 (60.8%) | 5,730 (100.0%) | — | 62.08 |
| 193 | 5,790 | 3,479 (60.1%) | 5,790 (100.0%) | — | 62.71 |
| 240 | 7,200 | 3,183 (44.2%) | 6,528 (90.7%) | — | — |

At the 240 fps target, the Isaac ROS publisher offered 237.37–238.25 fps across six blocks. Two of six fell outside the configured 1% tolerance. Raio offered 239.999–240.005 fps. Raio eventually delivered all 7,200 detections and transform batches. Its last detection in each block arrived 1.00–1.15 s after the last input. The stock detection monitor stopped after the 0.5 s grace period, so the experiment cannot distinguish late stock detections from dropped ones.

### Stock benchmark and matched-input comparisons

At a 191 fps target on this board, Raio delivered 191.3 detections/s versus 125.0 in NVIDIA’s unmodified benchmark, a 53% throughput gain. At 193 fps, the paired test delivered 193.3 detections/s for Raio and 115.2 for Isaac ROS, a 68% gain. The paired rates are medians across six five-second blocks, calculated from the intervals between consecutive detections. NVIDIA’s stock report divides the detection count by the elapsed time between first and last receipt and drops the highest and lowest of five blocks before averaging. Its monitor waits 5 s after playback. The paired monitor waits 0.5 s. The paired ROS controller also records publication times and counts transforms through an additional subscriber.

| Unmodified Isaac ROS 5.1 on this Orin | Reported input fps | Reported output fps |
| --- | ---: | ---: |
| [Default throughput search](stock/default.json) | 139.7 | 137.4 |
| [191 fps target](stock/fixed-191.json) | 190.3 | 125.0 |
| [240 fps target](stock/fixed-240.json) | 238.6 | 113.8 |

NVIDIA reports 191.0 detections/s at 193.0 input fps on its AGX Orin. Raio’s six 193 fps blocks ranged from 188.8 to 193.4 detections/s. The public report identifies the platform family. Its RAM and clock settings are unspecified, and the cause of the difference between the local and published Isaac ROS results is unresolved. [NVIDIA’s release report](https://github.com/NVIDIA-ISAAC-ROS/isaac_ros_benchmark/blob/release-5.1/results/isaac_ros_apriltag_node-agx_orin.json)

![AprilTag delivery by input rate](orin-51-delivery.png)

[Orin per-block measurements](orin-51-blocks.csv) · [Discovered ROS endpoints](ros-graph-endpoints.json) · [Graphviz source](isaac-ros-apriltag.dot)

The two high-rate sets (180/240 fps and 191/193 fps) each ran in reverse order on the second launch. To regenerate the table inputs and chart from two launch directories, run the following from `benchmark/isaac_apriltag/` with NumPy and Matplotlib installed.

```sh
python3 collect_blocks.py --launch first /trial/first --launch second /trial/second orin-51-blocks.csv
python3 plot_delivery.py orin-51-blocks.csv orin-51-delivery.png
```

## A40 replay results with Isaac ROS 4.6

Both detection and transform listeners were active. At each rate, both listeners in a given runtime received the same number of messages. Each delivery column therefore gives the count for one stream. P95 measures playback to detection receipt. A dash indicates that missing detections prevented reliable latency pairing.

| Target fps | Isaac ROS p95 (ms) | Raio p95 (ms) | Isaac ROS delivered | Raio delivered | Inputs |
|---:|---:|---:|---:|---:|---:|
| 30 | 7.86 | 3.43 | 450 | 450 | 450 |
| 60 | 5.29 | 3.18 | 900 | 900 | 900 |
| 120 | 6.58 | 3.29 | 1,800 | 1,800 | 1,800 |
| 180 | — | 2.98 | 2,695 | 2,700 | 2,700 |
| 240 | — | 3.29 | 2,346 | 3,600 | 3,600 |

The ten recorded frames repeat their original camera timestamps. Raio playback sets the image and its calibration in the same logical step. The stock detector matches the streams by timestamp on receipt. This comparison includes those input-handling choices.

## A40 repeat with Isaac ROS 5.1

Raio 0.2.0 and the stock ROS 2 Lyrical graph replayed ten 1280 × 720 images and their matching calibration. Each runtime published detections and transforms. In each measured block the detection and transform receivers recorded the same count. The tag IDs, corners and translations matched the ROS fixture. The maximum orientation difference was 0.000002 degrees. The 4.6 and 5.1 fixtures contained the same ten image digests at all five rates. The observer recorded three of those frames in a different order in the 180 fps fixture.

| Target fps | Isaac ROS 5.1 p95 (ms) | Raio p95 (ms) | Isaac ROS delivered | Raio delivered | Inputs |
|---:|---:|---:|---:|---:|---:|
| 30 | 7.66 | 3.33 | 450 | 450 | 450 |
| 60 | 4.86 | 3.18 | 900 | 900 | 900 |
| 120 | 5.28 | 3.26 | 1,800 | 1,800 | 1,800 |
| 180 | — | 3.16 | 2,697 | 2,700 | 2,700 |
| 240 | — | 3.00 | 2,597 | 3,600 | 3,600 |

At the 240 fps target, ROS offered 238.85, 240.06 and 234.99 fps in its three measured blocks. The third fell outside the controller's 1% target tolerance. The median delivered rates were 170.81 fps for ROS and 239.98 fps for Raio. ROS 4.6 delivered 2,346 of 3,600 in the earlier trial. These are separate benchmark sessions with different ROS playback implementations. A 30 fps control using the untouched 5.1 script also delivered all 450 measured detections. [A40 per-block measurements](a40-51-blocks.csv)

## Measured work

A separate observer captures the ten preprocessed frames, calibration messages and per-frame ROS detections in each stock launch. The controller captures them before timing on 4.6 and after timing on 5.1. The observer has no image subscription during the 5.1 timed blocks. The Raio launcher checks each image digest and preloads the immutable images into GPU memory. Its source publishes a CUDA IPC frame descriptor and a separate timestamped calibration value on native ports. The native detector reads matching values at the same logical time. It opens the GPU allocation once, uses the source's image pitch and intrinsics, synchronizes CUDA and publishes detections and transforms on two ports. Separate Raio processes receive the two outputs. The exporting process retains ownership until every child exits. Recorded data and static calibration are specific to this bounded playback test.

Both variants run one warm-up block followed by three measured five-second blocks at each target rate. Each retained ROS block uses the stock C++ composable graph, stock image delivery, playback and detection monitoring. The controller exports timestamps *after* monitoring stops. For the historical A40 tables, the stock clock starts after publishing the image and calibration group. The Raio clock starts after setting those two ports, before Raio flushes them to the downstream process at the end of its logical step. End time is monitor receipt. This measures the two implemented detection paths including their runtime overhead. Both detectors publish one detection array and one transform batch per image. Both graphs have separate detection and transform consumers. The latency endpoint is the detection consumer. ROS bag IO, image resizing, GPU preload and initialization are outside the measured interval. A playback target of 240 Hz requests 240 images per second from the ten-frame replay buffer.

The analysis checks every native output against the stock result for the same input stamp. Tolerances are 0.05 px maximum corner-coordinate difference, 1 mm translation norm and 0.1 degrees rotation difference using the absolute quaternion dot product. The fixture contains moving views of a single detected tag. The reported p95 is nearest rank within each retained block, then the median across blocks. The stock monitor uses independent sequence counters by default. Received and sent timestamps are paired in order only in complete blocks. Any loss suppresses the across-block p95 for that runtime/rate. Report counts and achieved rates even when latency cannot be paired. This fixed-rate test does not establish maximum sustainable throughput or live-camera latency.

## Transport and secondary outputs

All timed components run on one machine. Raio runs the publisher, detector and monitor in separate processes. Native port values travel over TCP loopback, including the image descriptor, calibration and detections. On A40 a CUDA IPC handle gives the detector access to the resident GPU pixels. On Orin the source shares a CUDA virtual-memory allocation using a file descriptor passed once over a local Unix socket. The detector reuses the mapped pixels for the run. The stock graph composes the playback, detector and monitor into one multithreaded process. It uses NITROS image input in 4.6 and CUDA-backed ROS 2 image messages in 5.1. The detection monitor uses a ROS 2 subscription. During the A40 4.6 240 Hz trial the process loaded `rmw_fastrtps_cpp` and Fast DDS 2.14.7. These observations identify the middleware and graph layout. They do not identify the lower-level delivery mechanism for every DDS message.

Each detector computes the pose once. The native node unconditionally sets its `tags` and `transforms` outputs in the same logical step. The transform port carries a sequence number and a batch of tuples (camera timestamp in nanoseconds, camera frame name, tag frame name, translation XYZ, quaternion XYZW). Tag frame names follow the stock convention, for example `tag36h11:16`. A frame with no detections produces an empty batch. Each consumer receives its port independently. An unwired transform output has no recipient.

The stock graph keeps its normal detection monitor. A separate ROS process listens on `/tf` during fixture collection and all timing blocks. The native graph has a separate transform monitor. The report checks both transform streams against the stock reference poses and camera frame names and reports their delivery counts separately from detection latency. This measures publication and delivery. There is no transform-tree query in the benchmark.

To exercise the native graph with an unwired transform port, append `--no-transform-listener` to the `native.py` invocation with a fresh output directory. The detector still sets both ports. The paired comparison uses a listener on each side.

## NVIDIA benchmark source

The paired launcher appends controller code to the packaged benchmark to capture a reference fixture and export timestamps. Isaac ROS 5.1 uses `BufferPlaybackNode` for the image and calibration pair. On Orin, the fixture observer captures the ten buffered frames after timing at 5 fps using reliable QoS. The cuAprilTags archives used in the 4.6 and 5.1 A40 runs share SHA-256 `a7f32f58782de61dd23e507fe6ce5a907379498deec505669fe339ecd4facba7`. [NVIDIA 5.1 benchmark](https://github.com/NVIDIA-ISAAC-ROS/isaac_ros_benchmark/blob/release-5.1/benchmarks/isaac_ros_apriltag_benchmark/scripts/isaac_ros_apriltag_node.py) · [NVIDIA 4.6 benchmark](https://github.com/NVIDIA-ISAAC-ROS/isaac_ros_benchmark/blob/release-4.6/benchmarks/isaac_ros_apriltag_benchmark/scripts/isaac_ros_apriltag_node.py) · [NVIDIA 4.6 detector](https://github.com/NVIDIA-ISAAC-ROS/isaac_ros_apriltag/blob/release-4.6/isaac_ros_apriltag/src/apriltag_node.cpp)
