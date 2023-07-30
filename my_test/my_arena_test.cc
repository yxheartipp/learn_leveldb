#include "util/my_arena.h"
#include <cstddef>

#include "gtest/gtest.h"
#include "util/random.h"

namespace leveldb {

TEST(MyArenaTest, Empty) {My_Arean my_arean;}

TEST(MyArenaTest, Simple) {
    std::vector<std::pair<size_t,char*>> allocated;
    My_Arean arena;
    const int N = 100000;
    size_t bytes = 0;
    Random rnd(301);
    for (int i = 0; i < N; i++) {
        size_t s;
        if (i % (N / 10) == 0) {
            s = i;
        } else {
            s = rnd.OneIn(4000) ? rnd.Uniform(6000) : (rnd.OneIn(10) ? rnd.Uniform(100): rnd.Uniform(20)); 
        }
        if (s == 0) {
            s = 1;
        }
        char* result;
        if (rnd.OneIn(10)) {
            result = arena.AllocateAligned(s);
        } else {
            result = arena.Allocate(s);
        }

        for (size_t b = 0; b < s; b++) {
            result[b] = i % 256;
        }
        bytes += s;
        allocated.push_back(std::make_pair(s,result));
        ASSERT_GE(arena.MemoryUsage(),bytes);
        if (i > N / 10) {
            ASSERT_LE(arena.MemoryUsage(), bytes * 1.10);
        }
    }
    for (size_t i = 0; i < allocated.size(); i++) {
        size_t num_bytes = allocated[i].first;
        const char* p = allocated[i].second;
        for (size_t b = 0; b < num_bytes; b++) {
            ASSERT_EQ(int(p[b]) & 0xFF, i % 256);
        }
    }
}
}

int main(int argc, char** argv) {
  printf("Running main() from %s\n", __FILE__);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}