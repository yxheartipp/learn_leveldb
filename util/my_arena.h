#include <atomic>
#include <cstddef>
#include <vector>
#include <cassert>
#include <deque>
#include <memory>

namespace leveldb {

class My_Arena {
 public:
  //My_Arena();

  My_Arena(const My_Arena&) = delete;
  My_Arena& operator=(const My_Arena) =delete;

  My_Arena(size_t block_size);

  static constexpr size_t kInlineSize = 2048;
  static constexpr size_t kMinBlockSize = 4096;
  static constexpr size_t kMaxBlockSize = 2u << 30;
  static constexpr unsigned kAlignUnit = alignof(std::max_align_t);

  ~My_Arena();

  char* Allocate(size_t bytes);
  char* AllocateAligned(size_t bytes);

  size_t MemoryUsage() const {
    return blocks_memory_ + blocks_.size() * sizeof(char*) - 
           alloc_bytes_remaining_;
  }

  size_t MemoryAllocateBytes() const { return blocks_memory_; }

  size_t AllocateAndUnused() const { return alloc_bytes_remaining_; }

  bool IsInlineBlock() const {
    return blocks_.empty();
  }

  static size_t OptimizeBlockSize(size_t block_size);

  private:
   alignas(std::max_align_t) char inline_block_[kInlineSize];
   const size_t kBlockSize;
   std::deque<std::unique_ptr<char[]>> blocks_;

   char* unaligned_alloc_ptr_ = nullptr;
   char* aligned_alloc_ptr_ = nullptr;
   size_t alloc_bytes_remaining_ = 0;

   char* AllocateFallback(size_t bytes, bool aligned);
   char* AllocateNewBlock(size_t blocks_bytes);
   size_t blocks_memory_ = 0;
};

inline char* My_Arena::Allocate(size_t bytes) {
  assert(bytes > 0);
  if (bytes <= alloc_bytes_remaining_) {
    unaligned_alloc_ptr_ -= bytes;
    alloc_bytes_remaining_ -= bytes;
    return unaligned_alloc_ptr_;
  }
  return AllocateFallback(bytes, false);
}

}