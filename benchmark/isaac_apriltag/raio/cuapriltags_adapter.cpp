// AprilTag benchmark adapter. One CUDA IPC allocation holds the immutable playback buffer.
#include <cuda_runtime_api.h>
#include <cuAprilTags.h>
#include <Eigen/Geometry>
#include <cstdint>
#include <cstring>
#include <memory>

#ifdef RAIO_ORIN_VMM
#include "orin_slabs.hpp"
#endif

struct Tag {
  int id;
  float corners[8], center[2], xyz[3], xyzw[4];
};
struct Detector {
  cuAprilTagsHandle handle{};
  cudaStream_t stream{};
  void* buffer{};
  int width{}, height{}, pitch{};
  size_t frames{};
  ~Detector() {
    if (handle) cuAprilTagsDestroy(handle);
    if (stream) cudaStreamDestroy(stream);
#ifdef RAIO_ORIN_VMM
    if (buffer) orin::release(buffer);
#else
    if (buffer) cudaIpcCloseMemHandle(buffer);
#endif
  }
};

extern "C" {
int upload(const void* data, size_t bytes, void** ptr, unsigned char* token) {
#ifdef RAIO_ORIN_VMM
  return orin::allocate(data, bytes, ptr, token);
#else
  cudaIpcMemHandle_t ipc;
  static_assert(sizeof(ipc) == 64);
  auto err = cudaMalloc(ptr, bytes);
  if (err != cudaSuccess) return err;
  err = cudaMemcpy(*ptr, data, bytes, cudaMemcpyHostToDevice);
  if (err == cudaSuccess) err = cudaIpcGetMemHandle(&ipc, *ptr);
  if (err != cudaSuccess) {
    cudaFree(*ptr);
    *ptr = nullptr;
    return err;
  }
  std::memcpy(token, &ipc, sizeof(ipc));
  return 0;
#endif
}
int release(void* ptr) {
#ifdef RAIO_ORIN_VMM
  return orin::release(ptr);
#else
  return cudaFree(ptr);
#endif
}
int prepare(const unsigned char* token, int w, int h, int pitch, int frames,
            const float* k, Detector** result) {
  auto d = std::make_unique<Detector>();
#ifdef RAIO_ORIN_VMM
  try { d->buffer = orin::open_slab(token); }
  catch (const std::exception &e) { std::fprintf(stderr, "%s\n", e.what()); return -1; }
#else
  cudaIpcMemHandle_t ipc;
  std::memcpy(&ipc, token, sizeof(ipc));
  auto err = cudaIpcOpenMemHandle(&d->buffer, ipc, cudaIpcMemLazyEnablePeerAccess);
  if (err != cudaSuccess) return err;
#endif
  auto stream_status = cudaStreamCreate(&d->stream);
  if (stream_status != cudaSuccess) return stream_status;
  cuAprilTagsCameraIntrinsics_t intrinsics{k[0], k[1], k[2], k[3]};
  int rc = nvCreateAprilTagsDetector(&d->handle, w, h, 4, NVAT_TAG36H11, &intrinsics, 0.22f);
  if (rc) return rc;
  d->width = w;
  d->height = h;
  d->pitch = pitch;
  d->frames = frames;
  *result = d.release();
  return 0;
}
int detect(Detector* d, unsigned index, Tag* out, uint32_t* count) {
  if (index >= d->frames) return -1;
  auto offset = size_t(index) * d->height * d->pitch;
  auto ptr = static_cast<unsigned char*>(d->buffer) + offset;
  cuAprilTagsImageInput_t image{reinterpret_cast<uchar3*>(ptr), size_t(d->pitch),
                                uint16_t(d->width), uint16_t(d->height)};
  cuAprilTagsID_t tags[64];
  int rc = cuAprilTagsDetect(d->handle, &image, tags, count, 64, d->stream);
  if (rc) return rc;
  rc = cudaStreamSynchronize(d->stream);
  if (rc) return rc;
  for (unsigned i = 0; i < *count; ++i) {
    const auto& tag = tags[i];
    auto& result = out[i];
    result.id = tag.id;
    const auto* corners = tag.corners;
    for (int j = 0; j < 4; ++j) {
      result.corners[2 * j] = corners[j].x;
      result.corners[2 * j + 1] = corners[j].y;
    }
    // Match the stock node's intersection of the two diagonals and its pose convention.
    float slope1 = (corners[2].y - corners[0].y) / (corners[2].x - corners[0].x);
    float slope2 = (corners[3].y - corners[1].y) / (corners[3].x - corners[1].x);
    float intercept1 = corners[0].y - slope1 * corners[0].x;
    float intercept2 = corners[3].y - slope2 * corners[3].x;
    result.center[0] = (intercept2 - intercept1) / (slope1 - slope2);
    result.center[1] = (slope2 * intercept1 - slope1 * intercept2) / (slope2 - slope1);
    std::memcpy(result.xyz, tag.translation, sizeof(result.xyz));
    Eigen::Map<const Eigen::Matrix3f> rotation(tag.orientation);
    Eigen::Quaternionf quaternion(rotation);
    std::memcpy(result.xyzw, quaternion.coeffs().data(), sizeof(result.xyzw));
  }
  return 0;
}
void finish(Detector* d) { delete d; }
}
