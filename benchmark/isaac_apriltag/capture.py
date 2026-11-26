"""Auxiliary observer of the stock graph. Exclude this run from latency statistics."""

import hashlib
import json
import os
import time
from pathlib import Path

import rclpy
from isaac_ros_apriltag_interfaces.msg import AprilTagDetectionArray
from rclpy.qos import qos_profile_sensor_data
from rclpy.serialization import serialize_message
from sensor_msgs.msg import CameraInfo, Image

out = Path(os.environ.get("FIXTURE", "/trial/fixture"))
out.mkdir()
images, calibration, detections = {}, {}, {}
order = []


def stamp(msg):
    return msg.header.stamp.sec * 10**9 + msg.header.stamp.nanosec


def image(msg):
    key = stamp(msg)
    if key in images:
        return
    data = bytes(msg.data)
    digest = hashlib.sha256(data).hexdigest()
    (out / (digest + ".bin")).write_bytes(data)
    images[key] = {
        "sha256": digest,
        "width": msg.width,
        "height": msg.height,
        "step": msg.step,
        "encoding": msg.encoding,
    }
    order.append(key)


def info(msg):
    calibration[stamp(msg)] = (list(msg.k), serialize_message(msg).hex())


def result(msg):
    tags = []
    for tag in msg.detections:
        pose = tag.pose.pose.pose
        tags.append(
            {
                "id": tag.id,
                "corners": [[c.x, c.y] for c in tag.corners],
                "xyz": [pose.position.x, pose.position.y, pose.position.z],
                "xyzw": [
                    pose.orientation.x,
                    pose.orientation.y,
                    pose.orientation.z,
                    pose.orientation.w,
                ],
            }
        )
    detections[stamp(msg)] = tags


rclpy.init()
node = rclpy.create_node("raio_fixture_capture")
for typ, topic, fn in (
    (Image, "image", image),
    (CameraInfo, "camera_info", info),
    (AprilTagDetectionArray, "apriltag_detections", result),
):
    node.create_subscription(typ, "/r2b/" + topic, fn, qos_profile_sensor_data)
try:
    until = time.monotonic() + 180
    while time.monotonic() < until:
        rclpy.spin_once(node, timeout_sec=0.2)
        complete = [k for k in order if k in calibration and k in detections]
        if len(complete) >= 10:
            rows = [
                dict(
                    stamp=k,
                    **images[k],
                    K=calibration[k][0],
                    camera_info_cdr=calibration[k][1],
                    tags=detections[k],
                )
                for k in complete[:10]
            ]
            (out / "manifest.json").write_text(json.dumps(rows, indent=2))
            print(
                [
                    (r["encoding"], r["width"], r["height"], len(r["tags"]))
                    for r in rows
                ],
                flush=True,
            )
            break
    else:
        raise TimeoutError(
            (
                len(images),
                len(calibration),
                len(detections),
                order[:5],
                list(detections)[:5],
            )
        )
finally:
    node.destroy_node()
    rclpy.shutdown()
