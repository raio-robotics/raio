#!/bin/bash
set -eu
cd "$(dirname "$0")"
cuda=/usr/local/cuda
distro=${ROS_DISTRO:-jazzy}
package=isaac_ros_nitros
if [[ "$distro" == lyrical ]]; then package=cuapriltags_vendor; fi
apr="/opt/ros/$distro/share/$package/cuapriltags"
flags=()
if { [[ -r /sys/devices/soc0/machine ]] && grep -qi Orin /sys/devices/soc0/machine; } ||
   { [[ -r /proc/device-tree/compatible ]] && tr '\0' '\n' </proc/device-tree/compatible | grep -qx nvidia,tegra234; }; then
    flags+=(-DRAIO_ORIN_VMM -L/usr/local/cuda/lib64/stubs -lcuda -pthread)
fi
g++ -O3 -std=c++17 -fPIC -shared cuapriltags_adapter.cpp -I/usr/include/eigen3 -I"$cuda/include" -I"$apr/cuapriltags" "$apr/lib/libcuapriltags.a" -L"$cuda/lib64" -Wl,-rpath,"$cuda/lib64" -lcudart "${flags[@]}" -o libbench.so
ldd libbench.so
