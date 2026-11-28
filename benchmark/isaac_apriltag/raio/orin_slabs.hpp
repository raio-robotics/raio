// Jetson Orin GPU allocations exported via CUDA VMM and a private Unix socket.
#pragma once
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cuda.h>
#include <cuda_runtime.h>
#include <memory>
#include <poll.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <unordered_map>

namespace orin {
inline void driver(CUresult status) {
  if (status != CUDA_SUCCESS) {
    const char *error = nullptr;
    cuGetErrorString(status, &error);
    throw std::runtime_error(error ? error : "CUDA driver error");
  }
}
struct Fd {
  int value{-1};
  ~Fd() {
    if (value >= 0)
      close(value);
  }
};
inline void os_check(bool ok, const char *what) {
  if (!ok)
    throw std::system_error(errno, std::generic_category(), what);
}
inline sockaddr_un address(const char *path) {
  sockaddr_un a{};
  a.sun_family = AF_UNIX;
  if (strlen(path) >= sizeof(a.sun_path))
    throw std::runtime_error("socket path too long");
  std::strcpy(a.sun_path, path);
  return a;
}
struct Slab {
  CUmemGenericAllocationHandle allocation{};
  CUdeviceptr ptr{};
  size_t bytes{};
  bool mapped{};
  Fd exported, listener;
  std::string directory, path;
  std::atomic<bool> stop{};
  std::thread server;
  ~Slab() {
    stop.store(true);
    if (server.joinable())
      server.join();
    if (!path.empty())
      unlink(path.c_str());
    if (!directory.empty())
      rmdir(directory.c_str());
    if (mapped)
      cuMemUnmap(ptr, bytes);
    if (ptr)
      cuMemAddressFree(ptr, bytes);
    if (allocation)
      cuMemRelease(allocation);
  }
  void map(CUmemLocation location) {
    driver(cuMemAddressReserve(&ptr, bytes, 0, 0, 0));
    driver(cuMemMap(ptr, bytes, 0, allocation, 0));
    mapped = true;
    CUmemAccessDesc access{location, CU_MEM_ACCESS_FLAGS_PROT_READWRITE};
    driver(cuMemSetAccess(ptr, bytes, &access, 1));
  }
  void publish_handle() {
    char name[] = "/tmp/raio-slab-XXXXXX";
    os_check(mkdtemp(name) != nullptr, "mkdtemp");
    directory = name;
    path = directory + "/fd";
    auto a = address(path.c_str());
    listener.value = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    os_check(listener.value >= 0, "socket");
    os_check(
        bind(listener.value, reinterpret_cast<sockaddr *>(&a), sizeof(a)) == 0,
        "bind");
    os_check(listen(listener.value, 4) == 0, "listen");
    server = std::thread([this] {
      pollfd request{listener.value, POLLIN, 0};
      while (!stop.load()) {
        if (poll(&request, 1, 100) <= 0)
          continue;
        Fd client{accept4(listener.value, nullptr, nullptr, SOCK_CLOEXEC)};
        if (client.value < 0)
          continue;
        iovec iov{&bytes, sizeof(bytes)};
        alignas(cmsghdr) char control[CMSG_SPACE(sizeof(int))]{};
        msghdr msg{};
        msg.msg_iov = &iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control;
        msg.msg_controllen = sizeof(control);
        auto *c = CMSG_FIRSTHDR(&msg);
        c->cmsg_level = SOL_SOCKET;
        c->cmsg_type = SCM_RIGHTS;
        c->cmsg_len = CMSG_LEN(sizeof(int));
        memcpy(CMSG_DATA(c), &exported.value, sizeof(int));
        sendmsg(client.value, &msg, MSG_NOSIGNAL);
      }
    });
  }
};
inline std::unordered_map<void *, std::unique_ptr<Slab>> slabs;
inline void *retain(std::unique_ptr<Slab> s) {
  auto ptr = reinterpret_cast<void *>(s->ptr);
  slabs.emplace(ptr, std::move(s));
  return ptr;
}
inline CUmemAllocationProp properties() {
  int device = 0;
  auto rc = cudaFree(nullptr);
  if (rc != cudaSuccess)
    throw std::runtime_error(cudaGetErrorString(rc));
  driver(cuCtxGetDevice(&device));
  CUmemAllocationProp prop{};
  prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
  prop.location = {CU_MEM_LOCATION_TYPE_DEVICE, device};
  prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;
  return prop;
}
inline int allocate(const void *host, size_t n, void **ptr, void *token) {
  try {
    auto s = std::make_unique<Slab>();
    auto prop = properties();
    size_t granularity;
    driver(cuMemGetAllocationGranularity(&granularity, &prop,
                                         CU_MEM_ALLOC_GRANULARITY_MINIMUM));
    s->bytes = ((n - 1) / granularity + 1) * granularity;
    driver(cuMemCreate(&s->allocation, s->bytes, &prop, 0));
    driver(cuMemExportToShareableHandle(
        &s->exported.value, s->allocation,
        CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR, 0));
    s->map(prop.location);
    if (host)
      driver(cuMemcpyHtoD(s->ptr, host, n));
    s->publish_handle();
    if (s->path.size() >= 64)
      throw std::runtime_error("descriptor too long");
    std::memset(token, 0, 64);
    std::memcpy(token, s->path.data(), s->path.size());
    *ptr = retain(std::move(s));
    return 0;
  } catch (const std::exception &e) {
    std::fprintf(stderr, "%s\n", e.what());
    return -1;
  }
}
inline void *open_slab(const void *token) {
  auto s = std::make_unique<Slab>();
  auto prop = properties();
  auto a = address(static_cast<const char *>(token));
  Fd client{socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0)};
  os_check(client.value >= 0, "socket");
  os_check(connect(client.value, reinterpret_cast<sockaddr *>(&a), sizeof(a)) ==
               0,
           "connect");
  iovec iov{&s->bytes, sizeof(s->bytes)};
  alignas(cmsghdr) char control[CMSG_SPACE(sizeof(int))]{};
  msghdr msg{};
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  msg.msg_control = control;
  msg.msg_controllen = sizeof(control);
  auto n = recvmsg(client.value, &msg, MSG_CMSG_CLOEXEC);
  auto *c = CMSG_FIRSTHDR(&msg);
  if (n != sizeof(s->bytes) || !c || c->cmsg_type != SCM_RIGHTS ||
      c->cmsg_level != SOL_SOCKET || c->cmsg_len != CMSG_LEN(sizeof(int)))
    throw std::runtime_error("invalid GPU allocation descriptor");
  Fd fd;
  memcpy(&fd.value, CMSG_DATA(c), sizeof(int));
  driver(cuMemImportFromShareableHandle(
      &s->allocation, reinterpret_cast<void *>(static_cast<intptr_t>(fd.value)),
      CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR));
  s->map(prop.location);
  return retain(std::move(s));
}
inline int release(void *ptr) { return slabs.erase(ptr) == 1 ? 0 : -1; }
} // namespace orin
