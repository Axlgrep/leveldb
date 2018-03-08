// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

// 这里做的是先解析当前要获取数据的长度，同时p跳过前面表示
// 数据长度的几个Byte, 然后p ~ p + len就是我们要获取数据的
// 区间
static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator& cmp)
    : comparator_(cmp),
      refs_(0),
      table_(comparator_, &arena_) {
}

MemTable::~MemTable() {
  assert(refs_ == 0);
}

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr)
    const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator: public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) { }

  virtual bool Valid() const { return iter_.Valid(); }
  virtual void Seek(const Slice& k) { iter_.Seek(EncodeKey(&tmp_, k)); }
  virtual void SeekToFirst() { iter_.SeekToFirst(); }
  virtual void SeekToLast() { iter_.SeekToLast(); }
  virtual void Next() { iter_.Next(); }
  virtual void Prev() { iter_.Prev(); }
  virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); }
  virtual Slice value() const {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  virtual Status status() const { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;       // For passing to EncodeKey

  // No copying allowed
  MemTableIterator(const MemTableIterator&);
  void operator=(const MemTableIterator&);
};

Iterator* MemTable::NewIterator() {
  return new MemTableIterator(&table_);
}

/*
 * 这个方法实际上是将SequenceNumber, ValueType, Key,
 * Value这些数据合并压缩到一个buf里面去，下面我们来
 * 看看这个buf的表现形式
 *
 * ******************************** Buf Format ********************************
 *
 * | <Internal Key Size> |      <Key>      | <SequenceNumber + ValueType> |   <Value Size>   |      <Value>      |
 *       1 ~ 5 Bytes        Key Size Bytes              8 Bytes               1 ~ 5 Bytes       Value Size Bytes
 *
 * Internal Key Size ： 这个存储的是Key的长度 + 存储SequenceNumber和ValueType所需的空间(8 Bytes)
 * 至于一个uint32_t类型的数据为什么要1 ~ 5 Bytes进行存储， 后面会细说
 * Key : 存储了Key的内容
 * SequenceNumber + ValueType: 这个用一个int64_t类型数字的0 ~ 7 Bytes存储ValueType,
 * 用8 ~ 63 Bytes存储SequenceNumber
 * Value Size : 存储的是Value的长度
 * Value :存储了Value的内容
 *
 * Q: 为什么一个uint32_t类型的数据要用1 ~ 5 Bytes进行存储, 而不是4 Bytes?
 * A: 看了leveldb的EncodeVarint32()方法之后对其有一个了解, 实际上Varint是
 * 一紧凑的数字表示方法, 它用一个或者多个Bytes来存储数字, 数值越小的数字可
 * 以用越少的Btyes进行存储, 这样能减少表示数字的字节数, 但是这种方法也有弊
 * 端, 如果一个比较大的数字可能需要5 Bytes才能进行存储.
 *
 * 原理: 一个Byte有8个字节, 在这种表示方法中最高位字节是一个状态位，而其余
 * 的7个字节则用于存储数据, 如果该Byte最高位字节为1, 则表示当前数字还没有
 * 表示完毕, 还需要下一个Byte参与解析, 如果该Byte最高位字节为0, 则表示数字
 * 部分已经解析完毕
 *
 * e.g..
 * 用varint数字表示方法来存储数字104
 * 104的二进制表现形式:  01101000
 * 使用varint数字表现形式只需1 Byte进行存储: 01101000
 * 这个Byte的最高位为0, 表示表示当前Byte已经是当前解析数字的末尾，
 * 剩余7位1101000是真实数据, 暂时保留
 * 将获取到的真实数据进行拼接 1101000 = 01101000
 * 01101000就是104的二进制表现形式
 *
 * 用varint数字表示方法来存储数字11880
 * 11880的二进制表现形式:  00101110 01101000
 * 使用varint数字表现形式需要2 Byte进行存储: 11101000 01011100
 * 其中第一个Byte中的最高位为1, 表示当前数字还没解析完毕还需后面的
 * Bytes参与解析, 剩余7位1101000是真实数据, 暂时保留
 * 第二个Byte中最高位为0, 表示当前Byte已经是当前解析数字的末尾,
 * 剩余7位1011100是真实数据, 暂时保留
 * 将两次获取的真实数据拼接起来 1011100 + 1101000 = 00101110 01101000
 * 00101110 01101000就是11880的二进制表现形式
 */

void MemTable::Add(SequenceNumber s, ValueType type,
                   const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len =
      VarintLength(internal_key_size) + internal_key_size +
      VarintLength(val_size) + val_size;
  char* buf = arena_.Allocate(encoded_len);
  char* p = EncodeVarint32(buf, internal_key_size);
  memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  memcpy(p, value.data(), val_size);
  assert((p + val_size) - buf == encoded_len);
  table_.Insert(buf);
}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  // 这个Iterator的实现在skiplist.h里面
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    // 这里是解析key的长度，然后取出key来和传入的key进行对比
    // 如果key是一样的则取出类型来进行比较，是kTypeValue
    // 还是kTypeDeletion
    const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8),
            key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

}  // namespace leveldb
