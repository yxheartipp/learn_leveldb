#include "leveldb/cache.h"
#include "leveldb/slice.h"

#include <bits/stdint-uintn.h>
#include <cassert>
#include <cstddef>
#include <cstdio>
#include <cstdlib>

#include "port/port.h"
#include "port/port_stdcxx.h"
#include "port/thread_annotations.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {
Cache::~Cache() {}

namespace {

struct M_LRUHandle {
  void* value;
  void (*deleter) (const Slice& key, void* value);
  M_LRUHandle* next_hash;
  M_LRUHandle* next;
  M_LRUHandle* prev;
  size_t charge;
  size_t key_length;
  bool in_cache;
  uint32_t refs;
  uint32_t hash;
  char key_data[1];

  Slice key() const {
    assert(next != this);
    return Slice(key_data, key_length);
  };


};

class M_HandleTable {
 public:
  M_HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
  ~M_HandleTable() { delete[] list_; }

  M_LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  M_LRUHandle* Insert(M_LRUHandle* h) {
    M_LRUHandle** ptr = FindPointer(h->key(), h->hash);
    M_LRUHandle* old = *ptr;
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    *ptr = h;
    if (old == nullptr) {
      ++elems_;
      if (elems_ > length_) {
        Resize();
      }
    }
    return old;
  }

  M_LRUHandle* Remove(const Slice& key, uint32_t hash) {
    M_LRUHandle** ptr = FindPointer(key, hash);
    M_LRUHandle* result = *ptr;
    if (result != nullptr) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  uint32_t length_;
  uint32_t elems_;
  M_LRUHandle** list_;

  M_LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    M_LRUHandle** ptr = &list_[hash & (length_) - 1];
    while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  void Resize() {
    uint32_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2;
    }
    M_LRUHandle** new_list = new M_LRUHandle*[new_length];
    uint32_t count = 0;
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    for (uint32_t i = 0; i < length_; i++) {
      M_LRUHandle* h = list_[i];
      while (h != nullptr) {
        M_LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        M_LRUHandle** ptr = &new_list[hash & (new_length) -1];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};


class M_LRUCache {
 public:
  M_LRUCache();
  ~M_LRUCache();

  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  void Prune();
  size_t TotalCharge() const {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(M_LRUHandle* e);
  void LRU_Append(M_LRUHandle* list, M_LRUHandle* e);
  void Ref(M_LRUHandle* e);
  void Unref(M_LRUHandle* e);
  bool FinishErase(M_LRUHandle* e) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  size_t capacity_;

  mutable port::Mutex mutex_;
  size_t usage_ GUARDED_BY(mutex_);

  M_LRUHandle lru_ GUARDED_BY(mutex_);
  M_LRUHandle in_use_ GUARDED_BY(mutex_);

  M_HandleTable table_ GUARDED_BY(mutex_);
};

M_LRUCache::M_LRUCache() : capacity_(0), usage_(0) {
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

M_LRUCache::~M_LRUCache() {
  assert(in_use_.next = &in_use_);
  for (M_LRUHandle* e = lru_.next; e != &lru_;) {
    M_LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);
    Unref(e);
    e = next;
  }
}

void M_LRUCache::Ref(M_LRUHandle* e) {
  if (e->refs == 1 && e->in_cache) {
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

void M_LRUCache::Unref(M_LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) {
    assert(!e->in_cache);
    (*e->deleter)(e->key(), e->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) {
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

void M_LRUCache::LRU_Remove(M_LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void M_LRUCache::LRU_Append(M_LRUHandle* list, M_LRUHandle* e) {
  e->next = list;
  e->next->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache::Handle* M_LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  M_LRUHandle* e = table_.Lookup(key, hash);
  if (e != nullptr) {
    Ref(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

void M_LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<M_LRUHandle*>(handle));
}

Cache::Handle* M_LRUCache::Insert(const Slice &key, uint32_t hash, void *value, 
                                  size_t charge, 
                                  void (*deleter)(const Slice& key, void *value)) {
  MutexLock l(&mutex_);
  M_LRUHandle* e = 
      reinterpret_cast<M_LRUHandle*>(malloc(sizeof(M_LRUHandle) - 1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->key_length = key.size();
  e->hash = hash;
  e->charge = charge;
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1;
  std::memcpy(e->key_data,key.data(), key.size());

  if (capacity_ > 0) {
    e->refs++;
    e->in_cache = true;
    LRU_Append(&in_use_, e);
    usage_ += charge;
    FinishErase(e);
  } else {
    e->next = nullptr;
  }
  while (usage_ > capacity_ && lru_.next != &lru_) {
    M_LRUHandle* old = lru_.next;
    assert(old->refs == 1);
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {
      assert(erased);
    }
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

bool M_LRUCache::FinishErase(M_LRUHandle* e) {
  if (e != nullptr) {
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }
  return e != nullptr;
}

void M_LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  FinishErase(table_.Remove(key, hash));
}

void M_LRUCache::Prune() {
  MutexLock l(&mutex_);
  while (lru_.next != &lru_) {
    M_LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) {
      assert(erased);
    }
  }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

class M_ShardedLRUCache : public Cache {
 private:
  M_LRUCache shard_[kNumShards];
};

}
}