// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

/*
 * **************** WriteBatch rep_ Format ****************
 * | <Sequence> | <Count> | <Record> |
 *     8 Bytes    4 Bytes
 *
 * Sequence: 实际上就是当前这个WriteBatch的一个序列号
 * Count: 这个数值表面后面record数组元素的数量
 *
 * ******************** Record Format ********************
 * | <ValueType> |   <Len>   |   <Data>   |
 *     1 Bytes      4 Bytes     Len Bytes
 *
 * 以上是Record数组中一个元素的表现形式，如果有多个元素实际上
 * 也是按照ValueType, Len, Data这种顺序进行排列的
 *
 * 从上图可以很清楚的看出WriteBatch中rep_的表现形式了，
 * 当前rep_的0 ~ 7 Bytes存储当前WriteBatch的一个序列号
 * 当前rep_的8 ~ 11 Bytes存储当前WriteBatch的Record数组中
 * 有多少个元素
 * 剩余的Bytes存储这个Record数组, 数组元素的表现形式如上图
 * Record format所示
 *
 * e.g..
 * 我们在一个WriteBatch中首先Put("key","v1"), 然后再Delete("key")
 * 那么该WriteBatch中rep_的表现形式如下:
 * | <Sequence> | <Count> | <ValueType1> | <Len1-1> | <Data1-1> | <Len1-2> | <Data1-2> |  <ValueType2>  | <Len2> | <Data2> |
 *     ****          2       kTypeValue       3          key         2          v1        kTypeDeletion     3        key
 */

#include "leveldb/write_batch.h"

#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "util/coding.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;

WriteBatch::WriteBatch() {
  Clear();
}

WriteBatch::~WriteBatch() { }

WriteBatch::Handler::~Handler() { }

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
}

size_t WriteBatch::ApproximateSize() {
  return rep_.size();
}

/*
 * 这个方法的作用实际上就是遍历当前WriteBatch中的
 * record数组，然后将其中的元素添加到memtable当中
 * 去
 */
Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(kHeader);
  Slice key, value;
  int found = 0;
  while (!input.empty()) {
    found++;
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {
          handler->Put(key, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key)) {
          handler->Delete(key);
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

// 获取rep_中8 ~ 11 Bytes中存储的数据，并将其转换成int32_t
// 类型(实际上就是获取的当前WriteBatch中record数组中元素的
// 数量
int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8);
}

// 在rep_的8 ~ 11 Bytes中存储当前WriteBatch中record数组中
// 元素的数量
void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);
}

// 获取当前WriteBatch的序列号
SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

// 设置当前WriteBatch的序列号
void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

void WriteBatch::Put(const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeValue));
  PutLengthPrefixedSlice(&rep_, key);
  PutLengthPrefixedSlice(&rep_, value);
}

void WriteBatch::Delete(const Slice& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeDeletion));
  PutLengthPrefixedSlice(&rep_, key);
}

namespace {
class MemTableInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence_;
  MemTable* mem_;

  virtual void Put(const Slice& key, const Slice& value) {
    mem_->Add(sequence_, kTypeValue, key, value);
    sequence_++;
  }
  virtual void Delete(const Slice& key) {
    mem_->Add(sequence_, kTypeDeletion, key, Slice());
    sequence_++;
  }
};
}  // namespace

Status WriteBatchInternal::InsertInto(const WriteBatch* b,
                                      MemTable* memtable) {
  MemTableInserter inserter;
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.mem_ = memtable;
  return b->Iterate(&inserter);
}

void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

// 这里实际上是将两个WriteBatch合并成一个(将src所指向的WriteBatch追加到dst所指
// 向的WriteBatch后面), 要做的事情有两个，第一要更新src中记录record数组数量的值
// 第二要将src中的record数组追加到dst的record数组后面去
void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

}  // namespace leveldb
