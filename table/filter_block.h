// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>
#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish

/*
 * 这个过滤器的作用是将我们添加的key进行一系列Hash算法然后压缩到一个result_
 * 字符串里面去，以后我们就可以根据这个result_快速的查找某一个key我们之前是
 * 否添加过, 大概是这样, 没有细看
 *
 * keys_是一个字符串, 我们新添加进来的key都依次追加到keys_字符串的后面
 * start_集合中元素的类型是整形, 存储的是keys_字符串中各个key的起始位置
 * tmp_keys_中的元素类型是Slice, Slice存储了指向keys_字符串中各个key的起始位置的指针以及当前key的长度
 *
 * e.g..
 *   我们有一组key: axl, neil, dire
 *
 *   下标:     0     1     2     3     4     5     6     7     8     9     10
 *   keys:  |  a  |  x  |  l  |  n  |  e  |  i  |  l  |  d  |  i  |  r  |  e  |
 *   地址:    0x0   0x1   0x2   0x3   0x4   0x5   0x6   0x7   0x8   0x9   0x10
 *
 *   start_:    [0][3][7]
 *   tmp_keys_: [Slice(0x0, 3)][Slice(0x3, 4)][Slice(0x7, 4)]
 *
 */
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice& key);
  Slice Finish();

 private:
  void GenerateFilter();

  const FilterPolicy* policy_;
  std::string keys_;              // Flattened key contents
  std::vector<size_t> start_;     // Starting index in keys_ of each key
  std::string result_;            // Filter data computed so far
  std::vector<Slice> tmp_keys_;   // policy_->CreateFilter() argument
  std::vector<uint32_t> filter_offsets_;

  // No copying allowed
  FilterBlockBuilder(const FilterBlockBuilder&);
  void operator=(const FilterBlockBuilder&);
};

class FilterBlockReader {
 public:
 // REQUIRES: "contents" and *policy must stay live while *this is live.
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);

 private:
  const FilterPolicy* policy_;
  const char* data_;    // Pointer to filter data (at block-start)
  const char* offset_;  // Pointer to beginning of offset array (at block-end)
  size_t num_;          // Number of entries in offset array
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file)
};

}

#endif  // STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
