#!/bin/bash
set -eu
cd "$(dirname "$0")"
cuda=/usr/local/cuda
apr=/opt/ros/jazzy/share/isaac_ros_nitros/cuapriltags
g++ -O3 -std=c++17 -fPIC -shared native_detector.cpp -I/usr/include/eigen3 -I"$cuda/include" -I"$apr/cuapriltags" "$apr/lib/libcuapriltags.a" -L"$cuda/lib64" -Wl,-rpath,"$cuda/lib64" -lcudart -o libbench.so
ldd libbench.so
