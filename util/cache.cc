// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "leveldb/cache.h"
#include "port/port.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache::~Cache() {
}

namespace {

// LRU cache implementation
//
// Cache entries have an "in_cache" boolean indicating whether the cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the cache.
//
// The cache keeps two linked lists of items in the cache.  All items in the
// cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the cache acquiring or losing its only
// external reference.

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
struct LRUHandle {
  void* value;                                // 由于value是个指针，指向的内容可能是我们自己在堆上
  void (*deleter)(const Slice&, void* value); // 分配的空间，所以我们要自己定义一个方法对其进行回收
  LRUHandle* next_hash;  // 用于指向HandleTable中其下一个LRUHandle的指针
  LRUHandle* next;       // next和prev是用于LRUCache中lru_或者in_use_环形双链表当中, 而
  LRUHandle* prev;       // 当前LRUHandle要么在lru_中，要么在in_use_中, 不可能同时存在
  size_t charge;      // TODO(opt): Only allow uint32_t?
  size_t key_length;
  bool in_cache;      // Whether entry is in the cache.
  uint32_t refs;      // References, including cache reference, if present.
  uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
  char key_data[1];   // Beginning of key

  Slice key() const {
    // next_ is only equal to this if the LRU handle is the list head of an
    // empty list. List heads never have meaningful keys.
    assert(next != this);

    return Slice(key_data, key_length);
  }
};


/*
 * HandleTable实际上就是一个数组, 数组的元素就是指向LRUHandle的指针，
 * 然后前一个LRUHandle又指向下一个LRUHandle(所以我们也可以说数组的每
 * 元素是一个链表的head)
 *
 *       |------------|
 *       |      0     | --> NULL
 *       |------------|
 *       |      1     | --> node1-1 -> node1-2 -> NULL
 *       |------------|
 *       |      2     | --> node2-1 -> NULL
 *       |------------|
 *       |     ...    |
 *       |------------|
 *       | length - 1 | --> NULL
 *       |------------|
 */
// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(NULL) { Resize(); }
  ~HandleTable() { delete[] list_; }

  // 返回指向HandleTable中与key和hash都匹配的结点的指针
  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  /* e.g..
   *
   *  head_ -> NULL
   *  这是一个空链表的初始状态, 这时候我们要插入一个node_a, 我们通过
   *  FindPointer()方法会返回一个指向head_的指针(head_本身就是一个指
   *  针,head_的初值为NULL), old = *ptr, 那么old == NULL, 接着我们再
   *  为结点的后继指针赋值, node_a->next_hash = NULL, (这里没有后继结
   *  点, 所以是NULL), 最后令*ptr = &node_a(在这里我们完全可以把*ptr
   *  等同于head_, 就是我们的head_指向了node_a.
   *
   *  插入第一个结点以后的链表为:
   *  head_ -> node_a -> NULL
   *
   *
   *  此时我再在该链表中插入第二个结点node_b1, 我们通过FindPointer()
   *  方法会返回一个指向node_a->next_hash的指针, 由于*ptr的值在当前
   *  实际上就是node_a->next_hash的值, 所以old == NULL, 然后我们再令
   *  node_b1->next_hash = NULL, 最后令*ptr = &node_b1, 我们就完成了
   *  node_b1的插入(*ptr = &node_b1相当于node_a->next_hash = node_b1)
   *
   *  插入第二个结点以后的链表为:
   *  head_ -> node_a -> node_b1 -> NULL
   *
   *  如果此时我们再插入一个与node_b1拥有相同key和hash的node_b2, 我们通过
   *  FindPointer()方法会也会返回一个指向node_a->next_hash的指针(由于链表中
   *  已经存在与我们当前插入结点key, hash相同的结点了, 所以没有遍历到链表
   *  的末尾就提前返回了), 此时old == &node_b1, 然后我们将node_b2的后继结
   *  点指针指向node_b1后继结点指针指向的内容, 由于node_b1->next_hash为NULL
   *  所以node_b2->next_hash也指向NULL, 最后令*ptr = &node_b2, 我们就完成
   *  了node_b2的插入(*ptr = &node_b2相当于node_a->next_hash = node_b2)
   *
   *  最后链表状态为:
   *  head_ -> node_a -> node_b2 -> NULL
   */

  LRUHandle* Insert(LRUHandle* h) {
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    h->next_hash = (old == NULL ? NULL : old->next_hash);
    *ptr = h;
    // old == NULL表示新添加节点，需要对elems_进行累加
    // 反之是替换之前的节点
    if (old == NULL) {
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
    if (result != NULL) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;
  uint32_t elems_;
  LRUHandle** list_;

  // 这是整个HandleTable最关键的一个方法, 作用是给出一个key和他
  // 与之对应的hash值, 在HandleTable中找出指向其所在的位置的指针,
  // 并且返回一个指向这个指针的指针
  //
  // 首先根据hash值和当前数组的长度信息进行位运算，得出当前给定的
  // key应该在哪个链表当中, 然后获取到这个链表的head.
  // 获取到这个链表的head之后我们遍历这个链表的每个结点, 逐一匹配
  // key和hash值.
  //
  // 返回的指针分为三种情况
  //
  // 空链表的情况
  // head_ -> NULL
  // 那么这时候就是返回一个指向head_指针的指针
  //
  // 目标结点在链表中已经存在的情况
  // head_ -> node_a -> node_b -> node_c -> NULL
  // 这时候我们如果要找node_b结点，那么返回的是
  // 一个指向node_a->next_hash指针的指针
  //
  // 目标结点在链表中不存在的情况
  // head_ -> node_a -> node_b -> node_c -> NULL
  // 这时候如果我们需要寻找node_d结点, 那么返回的
  // 是一个指向node_c->next_hash指针的指针
  //
  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != NULL &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  // 这就是一个扩容然后rehash的操作, 在HandleTable构造方法里会调用
  // Resize方法, 所以list_不会为空
  void Resize() {
    uint32_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2;
    }
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != NULL) {
        LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard of sharded cache.
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  void Prune();
  size_t TotalCharge() const {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle*list, LRUHandle* e);
  void Ref(LRUHandle* e);
  void Unref(LRUHandle* e);
  bool FinishErase(LRUHandle* e);

  // Initialized before use.
  size_t capacity_;

  // mutex_ protects the following state.
  mutable port::Mutex mutex_;
  size_t usage_;

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // Entries have refs==1 and in_cache==true.
  LRUHandle lru_;

  // Dummy head of in-use list.
  // Entries are in use by clients, and have refs >= 2 and in_cache==true.
  LRUHandle in_use_;

  HandleTable table_;
};

LRUCache::LRUCache()
    : usage_(0) {
  // Make empty circular linked lists.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

LRUCache::~LRUCache() {
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  for (LRUHandle* e = lru_.next; e != &lru_; ) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);  // Invariant of lru_ list.
    Unref(e);
    e = next;
  }
}

void LRUCache::Ref(LRUHandle* e) {
  if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) { // Deallocate.
    assert(!e->in_cache);
    (*e->deleter)(e->key(), e->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) {  // No longer in use; move to lru_ list.
    // 当e->in_cache && e->refs == 1的时候表示外界已经没有客户端引用
    // 这时候当前LRUHandle可能在in_use_中也有可能在lru_中
    // 在in_use_中的场景：先将其从in_use_中移除，然后将其插入lru_
    //                    当中, 成为lru_当前的newest entry
    // 在lru_中的场景: 将其从lru_环形双链表中一个较老的位置移到
    //                 较新的位置上, 成为lru_当前的newest entry
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != NULL) {
    Ref(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

Cache::Handle* LRUCache::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value)) {
  MutexLock l(&mutex_);

  // 因为LRUHandle中的key_data默认是一个Bytes，
  // 为了存下key, 还需要多分配(key.size() - 1)个Bytes
  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      malloc(sizeof(LRUHandle)-1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1;  // for the returned handle.
  memcpy(e->key_data, key.data(), key.size());

  if (capacity_ > 0) {
    e->refs++;  // for the cache's reference.
    e->in_cache = true;
    LRU_Append(&in_use_, e);
    usage_ += charge;
    // table_.Insert(e)返回非NULL，表示这次是
    // 替换节点，而不是新增节点，而返回值是指
    // 向被替换节点的指针
    FinishErase(table_.Insert(e));
  } else {  // don't cache. (capacity_==0 is supported and turns off caching.)
    // next is read by key() in an assert, so it must be initialized
    e->next = NULL;
  }

  // 在lru_环形双链表里的LRUHandle，它的refs值必然是1, 因为
  // 没有外界客户端对其进行引用, 这时候先调用table_.Remove()
  // 方法将这个节点从HandleTable中移除，然后再调用FinishErase
  // 方法将这个节点从lru_中移除, 最后调用deleter将这个节点进行
  // 析构，释放内存, 如此循环，直到usage_小于capacity_或者
  // lru_环形链表已经为空
  while (usage_ > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;
    assert(old->refs == 1);
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

// 调用FinishErase之前实际上当前LRUHandle已经从HandleTable中移除了, 而
// FinishErase的作用是将其从in_use_或者lru_中移除, 从而达到释放LRUCache
// 中capacity_的作用，如果从in_use_或者lru_中移除之后发现外界对当前LRUHandle
// 也没有引用了，在Unref中就会调用它的deleter释放空间

// If e != NULL, finish removing *e from the cache; it has already been removed
// from the hash table.  Return whether e != NULL.  Requires mutex_ held.
bool LRUCache::FinishErase(LRUHandle* e) {
  if (e != NULL) {
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }
  return e != NULL;
}

void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  FinishErase(table_.Remove(key, hash));
}

void LRUCache::Prune() {
  MutexLock l(&mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

class ShardedLRUCache : public Cache {
 private:
  LRUCache shard_[kNumShards];
  port::Mutex id_mutex_;
  uint64_t last_id_;

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  static uint32_t Shard(uint32_t hash) {
    return hash >> (32 - kNumShardBits);
  }

 public:
  explicit ShardedLRUCache(size_t capacity)
      : last_id_(0) {
    // ShardedLRUCache实际上就是内部维护了多个LRUCache而已,
    // 在数据插入和查找的时候首先对key进行Hash取模，确认
    // 数据在哪个LRUCache当中，调用对应LRUCache的方法
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  virtual ~ShardedLRUCache() { }
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  virtual Handle* Lookup(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  virtual void Release(Handle* handle) {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  virtual void Erase(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  virtual void* Value(Handle* handle) {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  virtual uint64_t NewId() {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  virtual void Prune() {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }
  virtual size_t TotalCharge() const {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};

}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) {
  return new ShardedLRUCache(capacity);
}

}  // namespace leveldb
