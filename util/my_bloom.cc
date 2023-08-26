#include <bits/stdint-uintn.h>
#include <cstddef>
#include <string>
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"

#include "leveldb/slice.h"
#include "hash.h"

namespace leveldb {

namespace {
static uint32_t M_BloomHash(const Slice& key) {
  return Hash(key.data(), key.size(), 0xbc9f1d34);
}

class M_BloomFilterPolicy : public FilterPolicy {
 public:
  explicit M_BloomFilterPolicy(int bits_per_key) : bits_per_key_(bits_per_key) {
    k_ = static_cast<size_t>(bits_per_key * 0.69);
    if (k_ < 1) k_ = 1;
    if (k_ > 30) k_ = 30;
  }

  const char* Name() const override { return "leveldb.BuiltinBliimFilter2"; }

  void CreateFilter(const Slice* keys, int n, std::string* dst) const override {
    size_t bits = n * bits_per_key_;

    if (bits < 64) bits = 64;

    size_t bytes = (bits + 7) / 8;
    bits = bytes * 8;

    const size_t init_size = dst->size();
    dst->resize(init_size + bytes, 0);
    dst->push_back(static_cast<char>(k_));
    char* array = &(*dst)[init_size];
    for (int i = 0; i < n; i++) {
      uint32_t h = M_BloomHash(keys[i]);
      const uint32_t delta = (h >> 17) | (h << 15);
      for (size_t j = 0; j < k_; j++) {
        const uint32_t bitpos = h % bits;
        array[bitpos / 8] |= (1 << (bitpos % 8));
        h += delta;
      }
    }
  }

  bool KeyMayMatch(const Slice& key, const Slice& bloom_filter) const override {
    const size_t len = bloom_filter.size();
    if (len < 2) return false;
    const char* array = bloom_filter.data();
    const size_t bits = (len - 1) * 8;

    const size_t k = array[len - 1];
    if (k > 30) {
        return true;
    }

    uint32_t h = M_BloomHash(key);
    const uint32_t delta = (h >> 17) | (h << 15);
    for (size_t j = 0; j < k; j++) {
        const uint32_t bitpos = h % bits;
        if ((array[bitpos / 8] & (1 << (bitpos % 8))) == 0 ) return false;
        h += delta;
    }
    return true;
  }

 private:
  size_t bits_per_key_;
  size_t k_;
};
}

const FilterPolicy* M_NewBloomFilterPolicy(int bits_per_key) {
    return new M_BloomFilterPolicy(bits_per_key);
}

}