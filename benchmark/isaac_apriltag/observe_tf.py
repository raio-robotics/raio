"""Record the stock detector's /tf messages from a separate ROS process."""

import json
import sys
import time
from pathlib import Path

import rclpy
from rclpy.qos import QoSProfile, ReliabilityPolicy
from tf2_msgs.msg import TFMessage

folder = Path(sys.argv[1])
records = []


def observe(message):
    receipt = time.time_ns()
    transforms = []
    for entry in message.transforms:
        stamp = entry.header.stamp
        xyz = entry.transform.translation
        xyzw = entry.transform.rotation
        transforms.append(
            (
                stamp.sec * 10**9 + stamp.nanosec,
                entry.header.frame_id,
                entry.child_frame_id,
                [xyz.x, xyz.y, xyz.z],
                [xyzw.x, xyzw.y, xyzw.z, xyzw.w],
            )
        )
    records.append((receipt, transforms))


rclpy.init(args=[])
node = rclpy.create_node("benchmark_transform_listener")
try:
    node.create_subscription(
        TFMessage,
        "/tf",
        observe,
        QoSProfile(depth=100, reliability=ReliabilityPolicy.RELIABLE),
    )
    (folder / "tf-ready").touch()
    while not (folder / "tf-stop").exists():
        rclpy.spin_once(node, timeout_sec=0.1)
    (folder / "transforms.json").write_text(json.dumps(records))
finally:
    node.destroy_node()
    rclpy.shutdown()
