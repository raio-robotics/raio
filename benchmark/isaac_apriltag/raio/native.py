"""Native Raio playback, NVIDIA CUDA AprilTag detector and detection monitor."""

import argparse
import ctypes as c
import hashlib
import json
import struct
import time
from pathlib import Path

import raio

ROOT = Path(__file__).resolve().parent
P = c.c_void_p


class Tag(c.Structure):
    _fields_ = [
        ("id", c.c_int),
        ("corners", c.c_float * 8),
        ("center", c.c_float * 2),
        ("xyz", c.c_float * 3),
        ("xyzw", c.c_float * 4),
    ]


def library():
    lib = c.CDLL(str(ROOT / "libbench.so"))
    lib.upload.argtypes = [P, c.c_size_t, c.POINTER(P), P]
    lib.release.argtypes = [P]
    lib.prepare.argtypes = [P, c.c_int, c.c_int, c.c_int, c.c_int, P, c.POINTER(P)]
    lib.detect.argtypes = [P, c.c_uint, c.POINTER(Tag), c.POINTER(c.c_uint)]
    lib.finish.argtypes = [P]
    lib.finish.restype = None
    return lib


def check(rc):
    if rc:
        raise RuntimeError(f"CUDA/detector returned {rc}")


def camera_frame(row):
    if "frame_id" in row:
        return row["frame_id"]
    # Earlier fixtures stored only the serialized CameraInfo. Its CDR1 header is
    # encapsulation, sec, nanosec, then a null-terminated frame name.
    raw = bytes.fromhex(row["camera_info_cdr"])
    if raw[:2] not in (b"\x00\x00", b"\x00\x01"):
        raise ValueError("unsupported CameraInfo CDR encapsulation")
    byte_order = "<" if raw[1] else ">"
    length = struct.unpack_from(byte_order + "I", raw, 12)[0]
    if not 0 < length <= len(raw) - 16 or raw[15 + length] != 0:
        raise ValueError("invalid CameraInfo frame name")
    return raw[16 : 15 + length].decode()


class Playback(raio.Node):
    image = raio.OutputPort[tuple]()
    calibration = raio.OutputPort[tuple]()

    def __init__(self, rate, count, folder, images, infos):
        self.rate, self.count, self.folder = rate, count, folder
        self.images, self.infos = images, infos

    @raio.task
    async def play(self):
        sent = []
        before_publish = []
        epoch = self.count // 4
        for i in range(self.count):
            if i % epoch == 0:
                if i:
                    deadline = time.monotonic() + 60
                    while not (self.folder / f"processed-{i}").exists():
                        if time.monotonic() >= deadline:
                            raise TimeoutError("prior benchmark block has not drained")
                        await raio.wait(raio.Time(10_000_000))
                await raio.wait(raio.Time(500_000_000))
            await raio.wait(raio.Time(round(1e9 / self.rate)))
            idx = i % len(self.images)
            packet = (i, *self.images[idx])
            before_publish.append(time.perf_counter_ns())
            self.image.set(packet)
            self.calibration.set(self.infos[idx])
            start = time.perf_counter_ns()
            sent.append(start)
        (self.folder / "before-publish.json").write_text(json.dumps(before_publish))
        (self.folder / "sent.json").write_text(json.dumps(sent))


class AprilTag(raio.Node):
    image = raio.InputPort[tuple]()
    calibration = raio.InputPort[tuple]()
    tags = raio.OutputPort[tuple]()
    transforms = raio.OutputPort[tuple]()

    @raio.task
    async def compute(self):
        lib = library()
        ctx = P()
        try:
            out = (Tag * 64)()
            count = c.c_uint()
            async for seq, stamp, index, token, w, h, pitch, nframes in self.image:
                assert self.calibration.present
                info_stamp, parent, k, cdr = self.calibration.value
                assert info_stamp == stamp and cdr
                if not ctx:
                    intrinsics = (c.c_float * 4)(k[0], k[4], k[2], k[5])
                    check(
                        lib.prepare(
                            token, w, h, pitch, nframes, intrinsics, c.byref(ctx)
                        )
                    )
                check(lib.detect(ctx, index, out, c.byref(count)))
                tags = []
                transforms = []
                for tag in out[: count.value]:
                    xyz, xyzw = list(tag.xyz), list(tag.xyzw)
                    tags.append(
                        (tag.id, list(tag.corners), xyz, xyzw, list(tag.center))
                    )
                    transforms.append((stamp, parent, f"tag36h11:{tag.id}", xyz, xyzw))
                self.tags.set((seq, stamp, tags))
                self.transforms.set((seq, transforms))
        finally:
            if ctx:
                lib.finish(ctx)


class Monitor(raio.Node):
    tags = raio.InputPort[tuple]()

    def __init__(self, folder, epoch):
        self.folder = folder
        self.epoch = epoch

    @raio.task
    async def observe(self):
        observed = []
        async for seq, stamp, tags in self.tags:
            end = time.perf_counter_ns()
            observed.append((seq, stamp, end, tags))
            if (seq + 1) % self.epoch == 0:
                (self.folder / f"processed-{seq + 1}").touch()
        (self.folder / "observed.json").write_text(json.dumps(observed))


class TransformMonitor(raio.Node):
    transforms = raio.InputPort[tuple]()

    def __init__(self, folder):
        self.folder = folder

    @raio.task
    async def observe(self):
        records = []
        async for seq, transforms in self.transforms:
            records.append((seq, time.perf_counter_ns(), transforms))
        (self.folder / "transforms.json").write_text(json.dumps(records))


def make_nodes(rate, count, folder, images, infos, listen_to_transforms=True):
    source = Playback(rate, count, folder, images, infos)
    detector = AprilTag()
    monitor = Monitor(folder, count // 4)
    raio.connect(source.image, detector.image)
    raio.connect(source.calibration, detector.calibration)
    raio.connect(detector.tags, monitor.tags)
    nodes = [source, detector, monitor]
    if listen_to_transforms:
        listener = TransformMonitor(folder)
        raio.connect(detector.transforms, listener.transforms)
        nodes.append(listener)
    return nodes


def run():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder", type=Path)
    parser.add_argument("rate", type=float)
    parser.add_argument("count", type=int)
    parser.add_argument("fixture", nargs="?", type=Path, default=ROOT.parent / "fixture")
    parser.add_argument("--no-transform-listener", action="store_true")
    args = parser.parse_args()
    folder, rate, count = args.folder, args.rate, args.count
    if rate <= 0 or count < 8 or count % 4:
        raise ValueError(
            "supply a positive rate and four equal blocks of at least two frames"
        )
    folder.mkdir()
    fixture = args.fixture
    rows = json.loads((fixture / "manifest.json").read_text())
    frames = []
    for row in rows:
        data = (fixture / (row["sha256"] + ".bin")).read_bytes()
        assert hashlib.sha256(data).hexdigest() == row["sha256"]
        assert (
            row["encoding"] in ("rgb8", "bgr8")
            and len(data) == row["step"] * row["height"]
        )
        assert all(
            row[k] == rows[0][k] for k in ("K", "width", "height", "step", "encoding")
        )
        frames.append(data)
    slab = b"".join(frames)
    lib = library()
    ptr = P()
    token = c.create_string_buffer(64)
    check(lib.upload(slab, len(slab), c.byref(ptr), token))
    try:
        images = [
            (r["stamp"], i, token.raw, r["width"], r["height"], r["step"], len(rows))
            for i, r in enumerate(rows)
        ]
        infos = [
            (r["stamp"], camera_frame(r), r["K"], bytes.fromhex(r["camera_info_cdr"]))
            for r in rows
        ]
        nodes = make_nodes(
            rate, count, folder, images, infos,
            listen_to_transforms=not args.no_transform_listener,
        )
        raio.run(*nodes)
    finally:
        check(lib.release(ptr))


if __name__ == "__main__":
    # Importing the node classes by module name keeps ctypes descriptors process-local.
    from native import run

    run()
