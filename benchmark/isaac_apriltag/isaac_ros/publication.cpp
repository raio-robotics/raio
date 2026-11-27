// Record the start and return of the packaged ROS playback call.
// The paired ROS monitor supplies the end timestamp on the same host system
// clock.
#include <array>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <dlfcn.h>
namespace {
using Clock = std::chrono::system_clock;
long long now() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             Clock::now().time_since_epoch())
      .count();
}
struct Span {
  long long start, end;
};
std::array<Span, 32000> samples;
size_t used = 0;
__attribute__((destructor)) void save() {
  if (!used)
    return;
  auto out = std::fopen(std::getenv("IMAGE_PUBLICATIONS"), "w");
  if (!out)
    std::abort();
  for (size_t i = 0; i < used; i++)
    std::fprintf(out, "%lld,%lld\n", samples[i].start, samples[i].end);
  std::fclose(out);
}
} // namespace
#define GROUP                                                                  \
  "_ZN14ros2_benchmark12PlaybackNode20PublishGroupMessagesEmRKN8std_"          \
  "msgs3msg7Header_ISaIvEEE"
extern "C" void group(void *, unsigned long, const void *) asm(GROUP);
void group(void *self, unsigned long index, const void *header) {
  static auto original = reinterpret_cast<decltype(&group)>(dlsym(
      dlopen("libbuffer_playback_node.so", RTLD_NOW | RTLD_LOCAL), GROUP));
  if (!original || used == samples.size())
    std::abort();
  auto &s = samples[used++];
  s.start = now();
  original(self, index, header);
  s.end = now();
}
