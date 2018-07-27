// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_MEMTABLE_H_
#define STORAGE_LEVELDB_DB_MEMTABLE_H_

#include <string>
#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/skiplist.h"
#include "util/arena.h"

namespace leveldb {

class InternalKeyComparator;
class Mutex;
class MemTableIterator;

class MemTable {
 public:
  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  explicit MemTable(const InternalKeyComparator& comparator);

  // Increase reference count.
  void Ref() { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  // Returns an estimate of the number of bytes of data in use by this
  // data structure. It is safe to call when MemTable is being modified.
  size_t ApproximateMemoryUsage();

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/format.{h,cc} module.
  Iterator* NewIterator();

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  void Add(SequenceNumber seq, ValueType type,
           const Slice& key,
           const Slice& value);

  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  bool Get(const LookupKey& key, std::string* value, Status* s);

 private:
  ~MemTable();  // Private since only Unref() should be used to delete it

  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) { }
    int operator()(const char* a, const char* b) const;
  };
  friend class MemTableIterator;
  friend class MemTableBackwardIterator;

  typedef SkipList<const char*, KeyComparator> Table;

  KeyComparator comparator_;
  int refs_;
  Arena arena_;
  Table table_;

  // No copying allowed
  MemTable(const MemTable&);
  void operator=(const MemTable&);
};

/*
 * ******************************** Buf Format ********************************
 *
 * Memtable Key
 *
 * | <Internal Key Size> |      <Key>      | <SequenceNumber + ValueType> |   <Value Size>   |      <Value>      |
 *       1 ~ 5 Bytes        Key Size Bytes              8 Bytes               1 ~ 5 Bytes       Value Size Bytes
 *
 * Internal Key
 * |     <Key>      | <SequenceNumber + ValueType> |
 *   Key Size Bytes              8 Bytes
 *
 * User Key
 * |     <Key>      |
 *   Key Size Bytes
 *
 *
 * 我们在打开Db的时候Options里会带上一个Compactor, 这个Compactor可以用于我们自己的key(对应于上面的User Key), 这个Compactor我们
 * 称为user_compactor由于我们的kv数据首先会插入到memtable中，所以leveldb首先会将kv进行编码(也就是上面的Memtable Key)成一个条目,
 * 然后将这个字符串条目插入到memtable中, 我们知道Memtable实际上内部就是由跳表实现， 那这个跳表内部必然要知道如何比较两个Memtable
 * Key的大小, 而提供这个功能的组件就是Memtabl内部的KeyComparator这个成员变量, 这个KeyComparator内部又有一个InternalKeyComparator,
 * 而InternalKeyComparator内部才持有我们的user_compactor, 听起来好像比较复杂， 我们下面来看一下各个Compactor的作用以及对Key的处理
 * 流程
 *
 * KeyComparator:
 *   KeyComparator内部的operator()接口传入两个Memtable Key, 内部调用GetLengthPrefixedSlice()方法从两个Memtable Key中获取到两个
 *   Internal Key, 由于KeyComparator内部持有了InterKeyComparator, 最后就会调用InterKeyComparator的compact方法来比较两个
 *   Internal Key的大小.
 *
 * InternalKeyComparator:
 *   InternalKeyComparator内部的Compare()接口传入两个Internal Key, 内部调用ExtractUserKey()方法从两个Internal Key中获取到两个
 *   User Key(实际上就是从Internal Key中获取除去最后8个字节的前面部分就是User Key, 可以参照上图), 由于InternalKeyComparator内
 *   部又持有一个user_compactor, 首先会调用user_compactor的compact方法来判断两个user_key的大小，如果两个user_key大小相等， 就
 *   会获取Internal Key的最后8个Bytes(也就是SequnceNumber + ValueType), 来比较其大小， 并且返回结果
 *
 * UserComparator:
 *   这个Comparaor实际上就是用比较User Key的，我们可以根据自己的Key的形式自己来指定这个Comparator, 如果没有指定， 默认是
 *   BytewiseComparator(), 也就是按照字典序来进行排列
 *
 *
 */

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_
