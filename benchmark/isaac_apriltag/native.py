"""Native Raio playback, NVIDIA CUDA AprilTag detector and detection monitor."""

import ctypes as c
import hashlib
import json
import sys
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


class Playback(raio.Node):
    image = raio.OutputPort[tuple]()
    calibration = raio.OutputPort[tuple]()

    def __init__(self, rate, count, folder, images, infos):
        self.rate, self.count, self.folder = rate, count, folder
        self.images, self.infos = images, infos

    @raio.task
    async def play(self):
        sent = []
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
            self.image.set((i, *self.images[idx]))
            self.calibration.set(self.infos[idx])
            start = time.perf_counter_ns()
            sent.append(start)
        (self.folder / "sent.json").write_text(json.dumps(sent))


class AprilTag(raio.Node):
    image = raio.InputPort[tuple]()
    calibration = raio.InputPort[tuple]()
    tags = raio.OutputPort[tuple]()

    @raio.task
    async def compute(self):
        lib = library()
        ctx = P()
        try:
            out = (Tag * 64)()
            count = c.c_uint()
            async for seq, stamp, index, token, w, h, pitch, nframes in self.image:
                assert self.calibration.present
                info_stamp, k, cdr = self.calibration.value
                assert info_stamp == stamp and cdr
                if not ctx:
                    intrinsics = (c.c_float * 4)(k[0], k[4], k[2], k[5])
                    check(
                        lib.prepare(
                            token, w, h, pitch, nframes, intrinsics, c.byref(ctx)
                        )
                    )
                check(lib.detect(ctx, index, out, c.byref(count)))
                self.tags.set(
                    (
                        seq,
                        stamp,
                        [
                            (
                                t.id,
                                list(t.corners),
                                list(t.xyz),
                                list(t.xyzw),
                                list(t.center),
                            )
                            for t in out[: count.value]
                        ],
                    )
                )
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


def run():
    folder = Path(sys.argv[1])
    rate = float(sys.argv[2])
    count = int(sys.argv[3])
    if rate <= 0 or count < 8 or count % 4:
        raise ValueError(
            "supply a positive rate and four equal blocks of at least two frames"
        )
    folder.mkdir()
    fixture = Path(sys.argv[4]) if len(sys.argv) > 4 else ROOT / "fixture"
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
            (r["stamp"], r["K"], bytes.fromhex(r["camera_info_cdr"])) for r in rows
        ]
        source = Playback(rate, count, folder, images, infos)
        detector = AprilTag()
        monitor = Monitor(folder, count // 4)
        raio.connect(source.image, detector.image)
        raio.connect(source.calibration, detector.calibration)
        raio.connect(detector.tags, monitor.tags)
        raio.run(source, detector, monitor)
    finally:
        check(lib.release(ptr))


if __name__ == "__main__":
    # Importing the node classes by module name keeps ctypes descriptors process-local.
    from native import run

    run()
