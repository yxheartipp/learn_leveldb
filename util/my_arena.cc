#include "util/my_arena.h"
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace leveldb {

static const int kBlockSize = 4096;

My_Arean::My_Arean()
    : alloc_ptr_(nullptr),alloc_bytes_remaining_(0),memory_usage_(0){}

My_Arean::~My_Arean() {
    for (size_t i = 0; i < blocks_.size(); i++) {
        delete[] blocks_[i];
    }
}

char* My_Arean::AllocateFallback(size_t bytes) {
    if (bytes > kBlockSize / 4) {
        char* result = AllocateNewBlock(bytes);
        return result;
    }

    alloc_ptr_ = AllocateNewBlock(kBlockSize);
    alloc_bytes_remaining_ = kBlockSize;

    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
}

char* My_Arean::AllocateAligned(size_t bytes) {
    const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
    static_assert((align & (align-1)) == 0,
                   "Pointer size should be a power of 2");
    size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align -1);
    size_t slop = (current_mod == 0 ? 0 : align - current_mod);
    size_t needed = bytes + slop;
    char* result;
    if (needed <= alloc_bytes_remaining_) {
        result = alloc_ptr_ + slop;
        alloc_ptr_ += needed;
        alloc_bytes_remaining_ -= needed;
    } else {
        result = AllocateFallback(bytes);
    }
    assert((reinterpret_cast<uintptr_t>(result) & (align-1)) == 0);
    return result;
}

char* My_Arean::AllocateNewBlock(size_t block_bytes) {
    char* result = new char[block_bytes];
    blocks_.push_back(result);
    memory_usage_.fetch_add(block_bytes + sizeof(char*), std::memory_order_release);
    return result;
}
}