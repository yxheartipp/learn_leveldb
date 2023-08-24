#include "util/my_arena.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include "util/arena.h"

namespace leveldb {

static const int kBlockSize = 4096;
//判断blocksize是否处于允许范围之内
size_t My_Arena::OptimizeBlockSize(size_t block_size) {
    block_size = std::max(My_Arena::kMinBlockSize, block_size);
    block_size = std::min(My_Arena::kMaxBlockSize, block_size);
 
    return block_size;
}

My_Arena::My_Arena(size_t block_size)
    : kBlockSize(OptimizeBlockSize(block_size)) {
  assert(kBlockSize >= kMinBlockSize && kBlockSize <= kMaxBlockSize);

  alloc_bytes_remaining_ = sizeof(inline_block_);
  blocks_memory_ += alloc_bytes_remaining_;
  aligned_alloc_ptr_ = inline_block_;
  unaligned_alloc_ptr_ = inline_block_ + alloc_bytes_remaining_;
}

My_Arena::~My_Arena() {
    while(!blocks_.empty()) {
        blocks_.pop_front();
    }
}

char* My_Arena::AllocateFallback(size_t bytes, bool aligned) {
    if (bytes > kBlockSize / 4) {
        char* result = AllocateNewBlock(bytes);
        return result;
    }
    size_t size = kBlockSize;
    char* block_head = AllocateNewBlock(size);
    alloc_bytes_remaining_ = size - bytes;
    
    if (aligned) {
        aligned_alloc_ptr_ = block_head + bytes;
        unaligned_alloc_ptr_ = block_head + size;
        return block_head;
    } else {
        aligned_alloc_ptr_ = block_head;
        unaligned_alloc_ptr_ = block_head + size - bytes;
        return unaligned_alloc_ptr_;
    }
}

char* My_Arena::AllocateAligned(size_t bytes) {
    //查看当前是否对齐
    size_t current_mod = reinterpret_cast<uintptr_t>(aligned_alloc_ptr_) & (kAlignUnit - 1);
    size_t slop = (current_mod == 0 ? 0 : kAlignUnit - current_mod);
    size_t needed = bytes + slop;
    char* result;
    if (needed <= alloc_bytes_remaining_) {
        result = aligned_alloc_ptr_ + slop;
        aligned_alloc_ptr_ += needed;
        unaligned_alloc_ptr_ -= needed;
    } else {
        result = AllocateFallback(bytes, true);
    }
    assert((reinterpret_cast<uintptr_t>(result) & (kAlignUnit - 1)) == 0);
    return result;
}

char* My_Arena::AllocateNewBlock(size_t blocks_bytes) {
    char* block = new char[blocks_bytes];
    blocks_.push_back(std::unique_ptr<char[]>(block));
    size_t allocated_size = blocks_bytes;
    blocks_memory_ += allocated_size;
    return block; 
}
}