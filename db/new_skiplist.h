#ifndef STORAGE_LEVELDB_DB__NEW_SKIPLIST_H_
#define STORAGE_LEVELDB_DB__NEW_SKIPLIST_H_


#include <atomic>
#include <cassert>
#include <cstdlib>

#include "util/arena.h"
#include "util/random.h"

namespace leveldb {

template <typename Key, class Comparator>
class New_SkipList {
 private:
  struct Node;

 public:
  explicit New_SkipList(Comparator cmp, Arena* arena);
  //禁止所有的拷贝构造函数
  New_SkipList(const New_SkipList&) = delete;
  New_SkipList& operator=(const New_SkipList&) = delete;

  void Insert(const Key& key);

  bool Contains(const Key& key) const;

  // Iteration
  class Iterator {
   public:
    explicit Iterator(const New_SkipList* list);

    bool Valid() const;

    const Key& key() const;

    void Next();

    void Prev();

    void Seek(const Key& target);

    void SeekToFirst();

    void SeekToLast();

   private:
    const New_SkipList* list_;
    Node* node_;
  };

 private:
  enum { kMaxHeight = 12 };

  inline int GetMaxHeight() const {
    return max_height_.load(std::memory_order_relaxed);
  }

  //新建节点
  Node* NewNode(const Key& key, int height);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // why use ** in this function
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  Node* FindLessThan(const Key& key) const;

  Node* FindLast() const;

  Comparator const compare_;
  Arena* const arena_;

  Node* const head_;

  std::atomic<int> max_height_;

  Random rnd_;
};

//按照模板实现Iterator以及Node
template <typename Key, class Comparator>
struct New_SkipList<Key, Comparator>::Node {
  explicit Node(const Key& k) : key(k) {}

  Key const key;

  Node* Next(int n) {
    assert(n >= 0);
    return next_[n].load(std::memory_order_acquire);
  }

  void SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_release);
  }

  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return next_[n].load(std::memory_order_relaxed);
  }
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_relaxed);
  }

 private:
  //为什么该数组只有一个Node
  std::atomic<Node*> next_[1];
};

template <typename Key, class Comparator>
typename New_SkipList<Key, Comparator>::Node*
New_SkipList<Key, Comparator>::NewNode(const Key& key, int height) {
  // 通过arena_申请出想要的内存，利用新的内存生成指针。
  char* const node_memory = arena_->AllocateAligned(
      sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
  return new (node_memory) Node(key);
}

template <typename Key, class Comparator>
inline New_SkipList<Key, Comparator>::Iterator::Iterator(
    const New_SkipList* list) {
  list_ = list;
  node_ = nullptr;
}

template <typename Key, class Comparator>
inline bool New_SkipList<Key, Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

template <typename Key, class Comparator>
// 通过常量成员函数直接进行限制。
inline const Key& New_SkipList<Key, Comparator>::Iterator::key() const {
  //安全校验防止内存地址空间的泄露。
  assert(Valid());
  return node_->key;
}

template <typename Key, class Comparator>
inline void New_SkipList<Key, Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template <typename Key, class Comparator>
inline void New_SkipList<Key, Comparator>::Iterator::Prev() {
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ == nullptr;
  }
}

template <typename Key, class Comparator>
inline void New_SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template <typename Key, class Comparator>
inline void New_SkipList<Key, Comparator>::Iterator::SeekToFirst() {
  //头节点是一个dummy 节点
  node_ = list_->head_->Next(0);
}

template <typename Key, class Comparator>
inline void New_SkipList<Key, Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ == nullptr;
  }
}

template <typename Key, class Comparator>
int New_SkipList<Key, Comparator>::RandomHeight() {
  static const unsigned int kNBranching = 4;
  int height = 1;
  // 使用rnd.OneIn 作为随机生成器，OneIn会保证1/kNBranching的概率不正常运行
  while (height < kMaxHeight && rnd_.OneIn(kNBranching)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

template <typename Key, class Comparator>
bool New_SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key,
                                                   Node* n) const {
  return (n != nullptr) && (compare_(n->key, key) < 0);
}

template <typename Key, class Comparator>
typename New_SkipList<Key, Comparator>::Node*
New_SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key,
                                                  Node** prev) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (KeyIsAfterNode(key, next)) {
      x = next;
    } else {
      if (prev != nullptr) prev[level] = x;
      if (level == 0) {
        return next;
      } else {
        level--;
      }
    }
  }
}

template <typename Key, class Comparator>
typename New_SkipList<Key, Comparator>::Node*
New_SkipList<Key, Comparator>::FindLessThan(const Key& key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(x->key, key) < 0);
    Node* next = x->Next(level);
    if (next == nullptr || compare_(next->key, key) >= 0) {
      if (level == 0) {
        return x;
      } else {
        level--;
      }
    } else {
      x = next;
    }
  }
}

template <typename Key, class Comparator>
typename New_SkipList<Key, Comparator>::Node*
New_SkipList<Key, Comparator>::FindLast() const {
  Node* x = head_;
  int levle = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(levle);
    if (next == nullptr) {
      if (levle == 0) {
        return x;
      } else {
        levle--;
      }
    } else {
      x = next;
    }
  }
}

//构造函数
template <typename Key, class Comparator>
New_SkipList<Key, Comparator>::New_SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0, kMaxHeight)),
      max_height_(1),
      rnd_(0xdeadbeef) {
  // 跳表对多层的支持每一层都要进行初始化。
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, nullptr);
  }
}

template <typename Key, class Comparator>
void New_SkipList<Key, Comparator>::Insert(const Key& key) {
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, prev);

  assert(x == nullptr || !Equal(key, x->key));

  int height = RandomHeight();
  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {
      prev[i] = head_;
    }
    max_height_.store(height, std::memory_order_relaxed);
  }
  x = NewNode(key, height);
  for (int i = 0; i < height; i++) {
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    prev[i]->SetNext(i, x);
  }
}

template <typename Key, class Comparator>
bool New_SkipList<Key, Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, nullptr);
  if (x != nullptr && Equal(key, x->key)) {
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb

#endif