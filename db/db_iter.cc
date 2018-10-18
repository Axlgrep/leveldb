// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_iter.h"

#include "db/filename.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace leveldb {

#if 0
static void DumpInternalIter(Iterator* iter) {
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey k;
    if (!ParseInternalKey(iter->key(), &k)) {
      fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
    } else {
      fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
    }
  }
}
#endif

namespace {

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
class DBIter: public Iterator {
 public:
  // Which direction is the iterator currently moving?
  // (1) When moving forward, the internal iterator is positioned at
  //     the exact entry that yields this->key(), this->value()
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().
  enum Direction {
    kForward,
    kReverse
  };

  DBIter(DBImpl* db, const Comparator* cmp, Iterator* iter, SequenceNumber s,
         uint32_t seed)
      : db_(db),
        user_comparator_(cmp),
        iter_(iter),
        sequence_(s),
        direction_(kForward),
        valid_(false),
        rnd_(seed),
        bytes_counter_(RandomPeriod()) {
  }
  virtual ~DBIter() {
    delete iter_;
  }
  virtual bool Valid() const { return valid_; }
  // 从InternalKey中获取出user_key进行返回
  virtual Slice key() const {
    assert(valid_);
    return (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_;
  }
  virtual Slice value() const {
    assert(valid_);
    return (direction_ == kForward) ? iter_->value() : saved_value_;
  }
  virtual Status status() const {
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  virtual void Next();
  virtual void Prev();
  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();

 private:
  void FindNextUserEntry(bool skipping, std::string* skip);
  void FindPrevUserEntry();
  bool ParseKey(ParsedInternalKey* key);

  inline void SaveKey(const Slice& k, std::string* dst) {
    dst->assign(k.data(), k.size());
  }

  inline void ClearSavedValue() {
    if (saved_value_.capacity() > 1048576) {
      std::string empty;
      swap(empty, saved_value_);
    } else {
      saved_value_.clear();
    }
  }

  // Pick next gap with average value of config::kReadBytesPeriod.
  ssize_t RandomPeriod() {
    return rnd_.Uniform(2*config::kReadBytesPeriod);
  }

  DBImpl* db_;
  const Comparator* const user_comparator_;
  Iterator* const iter_;
  SequenceNumber const sequence_;

  Status status_;
  std::string saved_key_;     // == current key when direction_==kReverse
  std::string saved_value_;   // == current raw value when direction_==kReverse
  Direction direction_;
  bool valid_;

  Random rnd_;
  ssize_t bytes_counter_;

  // No copying allowed
  DBIter(const DBIter&);
  void operator=(const DBIter&);
};

inline bool DBIter::ParseKey(ParsedInternalKey* ikey) {
  Slice k = iter_->key();
  ssize_t n = k.size() + iter_->value().size();
  bytes_counter_ -= n;
  while (bytes_counter_ < 0) {
    bytes_counter_ += RandomPeriod();
    db_->RecordReadSample(k);
  }
  if (!ParseInternalKey(k, ikey)) {
    status_ = Status::Corruption("corrupted internal key in DBIter");
    return false;
  } else {
    return true;
  }
}

void DBIter::Next() {
  assert(valid_);

  if (direction_ == kReverse) {  // Switch directions?
    direction_ = kForward;
    // iter_ is pointing just before the entries for this->key(),
    // so advance into the range of entries for this->key() and then
    // use the normal skipping code below.
    if (!iter_->Valid()) {
      iter_->SeekToFirst();
    } else {
      iter_->Next();
    }
    if (!iter_->Valid()) {
      valid_ = false;
      saved_key_.clear();
      return;
    }
    // saved_key_ already contains the key to skip past.
  } else {
    // 在调用迭代器的Next()的时候, 首先会记录一下当前的迭代器
    // 指向的user_key, 并且将其保存到saved_key_这个变量当中去
    // 接着调用下面的FindNextUserEntry的时候就会跳过和saved_key_
    // 相同的key, 因为最新的数据我们已经遍历到了(user_key相同
    // sequence number大的表示最新的数据)
    // Store in saved_key_ the current key so we skip it below.
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
  }

  FindNextUserEntry(true, &saved_key_);
}

/*
 * skip可以理解为我们已经获取了这个key了, 接下来如果还遍历到了这
 * 个key, 我们可以直接忽略掉, 因为之前获取的肯定sequence number
 * 要大, 也就是数据越新
 *
 * eg1..
 *   (D a) 18 -> (S a b) 14 -> NULL
 *   (S a c) 10 -> (S a f) 8 -> (S b d) 4 -> NULL
 *
 * 调用SeekToFirst之后迭代器会指向(S b d) 4
 *
 *
 * eg2..
 *   (S a b) 18 -> (S a c) 14 -> NULL
 *   (S a c) 10 -> (S b c) 8 -> (S b d) 4 -> NULL
 *
 * 调用SeekToFirst之后迭代器会指向(S a b) 18
 * 调用Next之后首先会记录一下a这个key, 然后遍历的时候
 * 直接跳过user_key为a的key, 最后迭代器会指向(S b c)
 *
 */

void DBIter::FindNextUserEntry(bool skipping, std::string* skip) {
  // Loop until we hit an acceptable entry to yield
  assert(iter_->Valid());
  assert(direction_ == kForward);
  do {
    ParsedInternalKey ikey;
    // 当ikey.sequence小于快照的sequence的时候才进行判断,
    // 否则直接跳过
    if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
      switch (ikey.type) {
        case kTypeDeletion:
          // Arrange to skip all upcoming entries for this key since
          // they are hidden by this deletion.
          SaveKey(ikey.user_key, skip);
          skipping = true;
          break;
        case kTypeValue:
          if (skipping &&
              user_comparator_->Compare(ikey.user_key, *skip) <= 0) {
            // Entry hidden
          } else {
            valid_ = true;
            saved_key_.clear();
            return;
          }
          break;
      }
    }
    iter_->Next();
  } while (iter_->Valid());
  saved_key_.clear();
  valid_ = false;
}

void DBIter::Prev() {
  assert(valid_);

  if (direction_ == kForward) {  // Switch directions?
    // iter_ is pointing at the current entry.  Scan backwards until
    // the key changes so we can use the normal reverse scanning code.
    assert(iter_->Valid());  // Otherwise valid_ would have been false
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
    while (true) {
      iter_->Prev();
      if (!iter_->Valid()) {
        valid_ = false;
        saved_key_.clear();
        ClearSavedValue();
        return;
      }
      if (user_comparator_->Compare(ExtractUserKey(iter_->key()),
                                    saved_key_) < 0) {
        break;
      }
    }
    direction_ = kReverse;
  }

  FindPrevUserEntry();
}

/*
 *
 * eg1..
 *   (D a) 18   -> (S a b) 14 -> NULL
 *   (S a c) 10 -> (S a f) 8  -> (S b d) 4 -> NULL
 *
 *  调用SeekToLast()之后迭代器会指向record (S a f) 8, 但是成员
 *  变量saved_key_和saved_value_会记录record (S b d) 4
 *  此时调用key(), value()返回的分别是b, d
 *
 *  调用Prev()之后此时iter_已经失效, 并且saved_key_和saved_value_
 *  里面不存储任何内容, 并且由于value_type == kTypeDeletion还会
 *  将DBIter的成员变量赋值为false, 宣告DBIter失效
 *
 *
 * eg2..
 *   (S a b) 20 -> (D a)  18 -> (S a c) 14 -> NULL
 *   (S a c) 10 -> (S b c) 8  -> (S b d) 4 -> NULL
 *
 *  调用SeekToLast()之后迭代器会指向record (S a c) 10, 但是成员
 *  变量saved_key_和saved_value_会记录record (S b c) 8, 此时调用
 *  key(), value()返回的分别是b, c
 *
 *  调用Prev()之后此时iter_已经失效，但是saved_key_和saved_value_
 *  会记录record(S a b) 20, 此时调用key(), vlaue()返回分别是a, b
 *  注意此时虽然iter_已经失效了, 但是DBIter的成员变量valid_被赋值为
 *  true, 所以外界调用迭代器的Valid()函数返回的是true
 *  再次调用Prev()之后, 由于下列函数一开始会将value_type设置为
 *  kTypeDeletion,并且由于iter_是失效的，所以直接走到最下面的逻辑
 *  将成员变量valid_赋值为false, 宣告DBIter失效
 *
 *
 * eg3..
 *   (S a b) 18 -> (S b d) 14 -> (S b f) 10 -> NULL
 *   (S a c) 9  -> (S b c) 7  -> NULL
 *
 *  调用SeekToLast()之后迭代器会指向(S a c) 9，但是成员变量
 *  saved_key_和saved_value_会记录record (S b d) 14
 *  此时调用key(), value()返回的分别是b, d
 *
 *  调用Prev()之后此时iter_已经失效, 但是saved_key_和saved_value_
 *  会记录record (S a b) 18, 此时调用key(), value()返回分别是a, b
 *  注意此时虽然iter_已经失效了, 但是DBIter的成员变量valid_被赋值为
 *  true, 所以外界调用迭代器的Valid()函数返回的是true
 *  再次调用Prev()之后, 由于下列函数一开始会将value_type设置为
 *  kTypeDeletion,并且由于iter_是失效的，所以直接走到最下面的逻辑
 *  将成员变量valid_赋值为false, 宣告DBIter失效
 */
void DBIter::FindPrevUserEntry() {
  assert(direction_ == kReverse);

  ValueType value_type = kTypeDeletion;
  if (iter_->Valid()) {
    do {
      ParsedInternalKey ikey;
      if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
        if ((value_type != kTypeDeletion) &&
            user_comparator_->Compare(ikey.user_key, saved_key_) < 0) {
          // We encountered a non-deleted value in entries for previous keys,
          break;
        }
        value_type = ikey.type;
        if (value_type == kTypeDeletion) {
          saved_key_.clear();
          ClearSavedValue();
        } else {
          Slice raw_value = iter_->value();
          if (saved_value_.capacity() > raw_value.size() + 1048576) {
            std::string empty;
            swap(empty, saved_value_);
          }
          SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
          saved_value_.assign(raw_value.data(), raw_value.size());
        }
      }
      iter_->Prev();
    } while (iter_->Valid());
  }

  if (value_type == kTypeDeletion) {
    // End
    valid_ = false;
    saved_key_.clear();
    ClearSavedValue();
    direction_ = kForward;
  } else {
    valid_ = true;
  }
}

void DBIter::Seek(const Slice& target) {
  direction_ = kForward;
  ClearSavedValue();
  saved_key_.clear();
  AppendInternalKey(
      &saved_key_, ParsedInternalKey(target, sequence_, kValueTypeForSeek));
  iter_->Seek(saved_key_);
  if (iter_->Valid()) {
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToFirst() {
  direction_ = kForward;
  ClearSavedValue();
  iter_->SeekToFirst();
  if (iter_->Valid()) {
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToLast() {
  direction_ = kReverse;
  ClearSavedValue();
  iter_->SeekToLast();
  FindPrevUserEntry();
}

}  // anonymous namespace

Iterator* NewDBIterator(
    DBImpl* db,
    const Comparator* user_key_comparator,
    Iterator* internal_iter,
    SequenceNumber sequence,
    uint32_t seed) {
  return new DBIter(db, user_key_comparator, internal_iter, sequence, seed);
}

}  // namespace leveldb
