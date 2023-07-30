#include <atomic>
#include <cstddef>
#include "util/arena.h"


namespace leveldb {

class My_Arean {
 public:
  My_Arean();

  My_Arean(const My_Arean&) = delete;
  My_Arean& operator=(const My_Arean) =delete;

  ~My_Arean();

  char* Allocate(size_t bytes);
  char* AllocateAligned(size_t bytes);

  size_t MemoryUsage() const {
    return memory_usage_.load(std::memory_order_relaxed);
  }

  private:
  char* AllocateFallback(size_t bytes);
  char* AllocateNewBlock(size_t block_bytes);

  char* alloc_ptr_;
  size_t alloc_bytes_remaining_;

  std::vector<char*> blocks_;
  std::atomic<size_t> memory_usage_;
};

inline char* My_Arean::Allocate(size_t bytes) {
  assert(bytes > 0);
  if (bytes <= alloc_bytes_remaining_) {
    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  return AllocateFallback(bytes);
}
}