#!/bin/bash
set -eu
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y --no-install-recommends curl gnupg ca-certificates
curl -fsSL https://isaac.download.nvidia.com/isaac-ros/repos.key | gpg --dearmor -o /usr/share/keyrings/isaac-bench.gpg
echo 'deb [signed-by=/usr/share/keyrings/isaac-bench.gpg] https://isaac.download.nvidia.com/isaac-ros/release-4.6 noble main' > /etc/apt/sources.list.d/isaac-bench.list
curl -fsSL https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2404/x86_64/cuda-keyring_1.1-1_all.deb -o /tmp/cuda-keyring.deb
dpkg -i /tmp/cuda-keyring.deb
curl -fsSL https://repo.download.nvidia.com/jetson/jetson-ota-public.asc | gpg --dearmor -o /usr/share/keyrings/jetson-bench.gpg
echo 'deb [signed-by=/usr/share/keyrings/jetson-bench.gpg] https://repo.download.nvidia.com/jetson/x86_64/noble r39.2 main' > /etc/apt/sources.list.d/jetson-bench.list
apt-get update
apt-get install -y --no-install-recommends ros-jazzy-isaac-ros-apriltag-benchmark libnvvpi4 cuda-cudart-dev-13-0 g++ python3-pip python3-numpy python3-pytest ros-jazzy-launch-testing-ros ros-jazzy-rosbag2-storage-sqlite3
