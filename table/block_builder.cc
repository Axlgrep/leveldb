// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"

namespace leveldb {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options),
      restarts_(),
      counter_(0),
      finished_(false) {
  assert(options->block_restart_interval >= 1);
  restarts_.push_back(0);       // First restart point is at offset 0
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);       // First restart point is at offset 0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
  return (buffer_.size() +                        // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +   // Restart array
          sizeof(uint32_t));                      // Restart array length
}

/*
 * 如果在这里采用紧凑类型的数字表示法，那么每个int32_t数据
 * 类型在当前block所占用的空间为1 ~ 5 Bytes大小不等，这样
 * 我们便无法准确获取到存储重启点数据的位置，按照以下方法
 * 就很好找了，我们读取当前block的最后4 Bytes，然后解析为
 * 一个int32_t类型的数字n，这个n就是我们重启点数组的大小,
 * 然后再往前读取4 * n Bytes, 这些就是重启点的数据了
 */
Slice BlockBuilder::Finish() {
  // Append restart array
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }
  PutFixed32(&buffer_, restarts_.size());
  finished_ = true;
  return Slice(buffer_);
}

/*
 * 这个方法的作用是向当前的block中添加一条Entry，由于我们调用这个方法添加key
 * 实际上是字典有序的(从memtable中遍历得到的自然是有序的), 所以前一个添加的key
 * 和后一个key可能会存在"部分前缀的重叠"(abcdd和abccc重叠部分就是abc), 为了节
 * 约空间, 后一个key可以只存储和前一个key不同的部分(这个例子中后一个key只需要
 * 存储cc即可), 这种做法有利有弊，既然只存储了和前一个key不同的部分，那么我们
 * 需要一些额外的空间来存储一些其他的数据，比如说共享长度(shared), 非共享长度
 * (non_shared)等等 这些数据都是用一个int32_t类型的数字进行存储,在memtable.cc
 * 中提到过, 这是一种紧凑型的数字表示法, 下面附上的记录在内存中的表现形式和example.
 *
 * e.g..
 *
 * key: Axl     value: vv
 * key: Axlaa   value: vv
 * key: Axlab   value: vv
 * key: Axlbb   value: vv
 *
 * ******************************** Entry Format ********************************
 * |   <Shared>   |   <Non Shared>   |   <Value Size>   |   <Unprefixed Key>   |     <Value>     |
 *   1 ~ 5 Bytes      1 ~ 5 Bytes        1 ~ 5 Bytes        non_shared Bytes     value_size Bytes
 *
 *       0                3                  2                   Axl                    vv
 *       3                2                  2                   aa                     vv
 *       4                1                  2                   b                      vv
 *       3                2                  2                   bb                     vv
 *
 *
 *  (真实的key, 尾部还会带上SequenceNumber和ValueType，这里为了example更清晰便没有展示出来)
 *
 *
 *  这里还有一重启点(restarts_)的概念, 重启点存在的目的是为了避免最开始的记录损坏,
 *  而其后面的所有数据都无法恢复的情况发生, 为了降低这个风险, leveldb引入了重启点,
 *  也就是每隔固定的条数(block_restart_interval)的Entry, 都强制加入一个重启点, 重
 *  启点指向的Entry会完整的记录自身的key(shared为0, 不再依赖上一条Entry, 前面的Entry
 *  损坏也不会影响重启点指向的Entry的读取)
 *
 *  当前所有的重启点会有序的记录在restarts_集合当中, 最后Flush到文件的时候这个重启点
 *  集合以及集合大小会写在当前block的尾部.
 */

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  Slice last_key_piece(last_key_);
  assert(!finished_);
  assert(counter_ <= options_->block_restart_interval);
  assert(buffer_.empty() // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);
  size_t shared = 0;
  if (counter_ < options_->block_restart_interval) {
    // See how much sharing to do with previous string
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    }
  } else {
    // Restart compression
    restarts_.push_back(buffer_.size());
    counter_ = 0;
  }
  const size_t non_shared = key.size() - shared;

  // Add "<shared><non_shared><value_size>" to buffer_
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);
  PutVarint32(&buffer_, value.size());

  // Add string delta to buffer_ followed by value
  buffer_.append(key.data() + shared, non_shared);
  buffer_.append(value.data(), value.size());

  // Update state
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(Slice(last_key_) == key);
  counter_++;
}

}  // namespace leveldb
