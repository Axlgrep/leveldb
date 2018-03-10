// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <stdint.h>
#include "leveldb/comparator.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"

namespace leveldb {

Comparator::~Comparator() { }

namespace {
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() { }

  virtual const char* Name() const {
    return "leveldb.BytewiseComparator";
  }

  virtual int Compare(const Slice& a, const Slice& b) const {
    return a.compare(b);
  }

  /*
   * 每当一个Data Block写完之后就会调用这个函数(除开
   * 最后一个Block), 这个函数的作用是计算出一个index_key.
   *
   * 我们假设当前Data Block中最大的Key为cur_max_key;
   * 假设下一个Data Block中最小的Key为next_min_key;
   * 那么这个index_key满足的条件为:
   *   cur_max_key <= index_key < next_min_key
   *
   * Q: 为什么不直接调用下面那个FindShortSuccessor函数
   *    计算这个索引Key.
   *
   * A: 实际上这是为了节省空间并且避免出错, 考虑下面的例子
   * e.g..
   *    cur_max_key = abcdef
   *    next_min_key = abf
   *    那么利用FindShortestSeparator计算出来的index_key = abd
   *    那么利用FindShortSuccessor计算出来的index_key = abcdeg
   *    abcdef <= abd < abf        符合要求
   *    abcdef <= abcdeg < abf     符合要求
   * e.g..
   *    cur_max_key = abc
   *    next_min_key = abcdef
   *    那么利用FindShortestSeparator计算出来的index_key = abc
   *    那么利用FindShortSuccessor计算出来的index_key = abd
   *    abc <= abc < abcdef   符合要求
   *    abc <= abd < abcdef   这是不符合要求的, 因为abd并不小于abcdef
   */
  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const {
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
        (*start)[diff_index]++;
        start->resize(diff_index + 1);
        assert(Compare(*start, limit) < 0);
      }
    }
  }

  /*
   * 当最后一个Data Block写完之后调用这个函数计算index_key.
   *
   * 我们假设最后的Data Block中最大的Key为fin_max_key;
   * 那么这个index_key满足的条件为:
   *   fin_max_key <= index_key
   */
  virtual void FindShortSuccessor(std::string* key) const {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i+1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
};
}  // namespace

static port::OnceType once = LEVELDB_ONCE_INIT;
static const Comparator* bytewise;

static void InitModule() {
  bytewise = new BytewiseComparatorImpl;
}

const Comparator* BytewiseComparator() {
  port::InitOnce(&once, InitModule);
  return bytewise;
}

}  // namespace leveldb
