// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/new_skiplist.h"
#include "db/skiplist.h"
#include <atomic>
#include <set>

#include "gtest/gtest.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/arena.h"
#include "util/hash.h"
#include "util/random.h"
#include "util/testutil.h"

namespace leveldb {

typedef uint64_t Key;

struct Comparator {
  int operator()(const Key& a, const Key& b) const {
    if (a < b) {
      return -1;
    } else if (a > b) {
      return +1;
    } else {
      return 0;
    }
  }
};

TEST(SkipTest, Empty) {
  Arena arena;
  Comparator cmp;
  New_SkipList<Key, Comparator> list(cmp, &arena);
  //SkipList<Key, Comparator> list(cmp, &arena);
  
  ASSERT_TRUE(!list.Contains(10));

  New_SkipList<Key, Comparator>::Iterator iter(&list);
  // SkipList<Key, Comparator>::Iterator iter(&list);
  ASSERT_TRUE(!iter.Valid());
  iter.SeekToFirst();
  ASSERT_TRUE(!iter.Valid());
  iter.Seek(100);
  ASSERT_TRUE(!iter.Valid());
  iter.SeekToLast();
  ASSERT_TRUE(!iter.Valid());
  Key key = 10;
  list.Insert(key);
  key = 30;
  list.Insert(key);
}

TEST(SkipTest, InsertAndLookup) {
  const int N = 2000;
  const int R = 5000;
  Random rnd(1000);
  std::set<Key> keys;
  Arena arena;
  Comparator cmp;
  SkipList<Key, Comparator> list(cmp, &arena);
  for (int i = 0; i < N; i++) {
    Key key = rnd.Next() % R;
    if (keys.insert(key).second) {
      list.Insert(key);
    }
  }

  for (int i = 0; i < R; i++) {
    if (list.Contains(i)) {
      ASSERT_EQ(keys.count(i), 1);
    } else {
      ASSERT_EQ(keys.count(i), 0);
    }
  }

  // Simple iterator tests
  {
    SkipList<Key, Comparator>::Iterator iter(&list);
    ASSERT_TRUE(!iter.Valid());

    iter.Seek(0);
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.begin()), iter.key());

    iter.SeekToFirst();
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.begin()), iter.key());

    iter.SeekToLast();
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.rbegin()), iter.key());
  }

  // Forward iteration test
  for (int i = 0; i < R; i++) {
    SkipList<Key, Comparator>::Iterator iter(&list);
    iter.Seek(i);

    // Compare against model iterator
    std::set<Key>::iterator model_iter = keys.lower_bound(i);
    for (int j = 0; j < 3; j++) {
      if (model_iter == keys.end()) {
        ASSERT_TRUE(!iter.Valid());
        break;
      } else {
        ASSERT_TRUE(iter.Valid());
        ASSERT_EQ(*model_iter, iter.key());
        ++model_iter;
        iter.Next();
      }
    }
  }

  // Backward iteration test
  {
    SkipList<Key, Comparator>::Iterator iter(&list);
    iter.SeekToLast();

    // Compare against model iterator
    for (std::set<Key>::reverse_iterator model_iter = keys.rbegin();
         model_iter != keys.rend(); ++model_iter) {
      ASSERT_TRUE(iter.Valid());
      ASSERT_EQ(*model_iter, iter.key());
      iter.Prev();
    }
    ASSERT_TRUE(!iter.Valid());
  }
}

}  // namespace leveldb


int main(int argc,char** argv   ) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}