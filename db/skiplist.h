// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <assert.h>
#include <stdlib.h>
#include "port/port.h"
#include "util/arena.h"
#include "util/random.h"

namespace leveldb {

class Arena;

template<typename Key, class Comparator>
class SkipList {
 private:
  struct Node;

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit SkipList(Comparator cmp, Arena* arena);

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(const Key& key);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Key& key) const;

  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const SkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

   private:
    const SkipList* list_;
    Node* node_;
    // Intentionally copyable
  };

 private:
  enum { kMaxHeight = 12 };

  // Immutable after construction
  Comparator const compare_;
  Arena* const arena_;    // Arena used for allocations of nodes

  Node* const head_;

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  port::AtomicPointer max_height_;   // Height of the entire list

  inline int GetMaxHeight() const {
    return static_cast<int>(
        reinterpret_cast<intptr_t>(max_height_.NoBarrier_Load()));
  }

  // Read/written only by Insert().
  Random rnd_;

  Node* NewNode(const Key& key, int height);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return NULL if there is no such node.
  //
  // If prev is non-NULL, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  Node* FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  Node* FindLast() const;

  // No copying allowed
  SkipList(const SkipList&);
  void operator=(const SkipList&);
};

// Implementation details follow
template<typename Key, class Comparator>
struct SkipList<Key,Comparator>::Node {
  explicit Node(const Key& k) : key(k) { }

  Key const key;

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  Node* Next(int n) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return reinterpret_cast<Node*>(next_[n].Acquire_Load());
  }
  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    next_[n].Release_Store(x);
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return reinterpret_cast<Node*>(next_[n].NoBarrier_Load());
  }
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].NoBarrier_Store(x);
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  port::AtomicPointer next_[1];
};

/*
 * sizeof(Node)大小由两部分构成, 一个成员变量是Key的大小, 另外一个
 * 是port::AtomicPointer数组的大小(next_[1]), 再追加分配
 * sizeof(port::AtomicPointer) * (height - 1)大小的空间是直接追加
 * 到Node()结构体后面的，所以Node结构体中大小为1的port::AtomicPointer
 * 数组就和后面分配的多个AtomicPointer对象连接起来形成了一个更大的数组
 *
 * e.g..
 * 一个高度为3的Node
 * |   <Key>   |     <AtomicPointer1>     |     <AtomicPointer2>     |     <AtomicPointer3>     |
 * ^                                      ^                                                     ^
 * |------------sizeof(Node)--------------|-----sizeof(port::AtomicPointer) * (height - 1)------|
 *
 * 其中<Key>和<AtomicPointer1>大小加在一起是sizeof(Node)
 * <AtomicPointer2>和<AtomicPointer3>大小加在一起是
 * sizeof(port::AtomicPointer) * (3 - 1)
 * 而这段内存空间就可以表现为一个高度为3的Node
 */
template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node*
SkipList<Key,Comparator>::NewNode(const Key& key, int height) {
  char* mem = arena_->AllocateAligned(
      sizeof(Node) + sizeof(port::AtomicPointer) * (height - 1));
  return new (mem) Node(key);
}

template<typename Key, class Comparator>
inline SkipList<Key,Comparator>::Iterator::Iterator(const SkipList* list) {
  list_ = list;
  node_ = NULL;
}

template<typename Key, class Comparator>
inline bool SkipList<Key,Comparator>::Iterator::Valid() const {
  return node_ != NULL;
}

template<typename Key, class Comparator>
inline const Key& SkipList<Key,Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = NULL;
  }
}

// Seek实际上就是找到跳表中第一个值大于等于target的node
template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, NULL);
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = NULL;
  }
}

template<typename Key, class Comparator>
int SkipList<Key,Comparator>::RandomHeight() {
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

template<typename Key, class Comparator>
bool SkipList<Key,Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
  // NULL n is considered infinite
  return (n != NULL) && (compare_(n->key, key) < 0);
}

template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node* SkipList<Key,Comparator>::FindGreaterOrEqual(const Key& key, Node** prev)
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (KeyIsAfterNode(key, next)) {
      // Keep searching in this list
      x = next;
    } else {
      if (prev != NULL) prev[level] = x;
      if (level == 0) {
        return next;
      } else {
        // Switch to next list
        level--;
      }
    }
  }
}

template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node*
SkipList<Key,Comparator>::FindLessThan(const Key& key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(x->key, key) < 0);
    Node* next = x->Next(level);
    if (next == NULL || compare_(next->key, key) >= 0) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node* SkipList<Key,Comparator>::FindLast()
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == NULL) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

// 实际上Node的结构十分简单, 有一个存储数据的成员变量key
// 还有一个指针数组next_[]，数组的成员是指向后继Node节点的
template<typename Key, class Comparator>
SkipList<Key,Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight)),
      max_height_(reinterpret_cast<void*>(1)),
      rnd_(0xdeadbeef) {
  for (int i = 0; i < kMaxHeight; i++) {
    // 令指针数组的第i个元素指向NULL
    // 这里实际上就是给大小为kMaxHeight的数组
    // 赋初始值NULL
    head_->SetNext(i, NULL);
  }
}

/*  ——
 * |11|
 * |10|
 * | 9|
 * | 8|
 * | 7|
 * | 6|
 * | 5|
 * | 4|
 * | 3|
 * | 2| ---------------------\   |2| --------> NULL
 * | 1| --------\  |1|--------\  |1| --------> NULL
 *  --       -      -      -      -      -
 * head_ -> "a" -> "b" -> "c" -> "e" -> "f" -> NULL
 *
 *  leveldb中的skiplist最高层数为12(kMaxHeight为12), 上图所示的
 *  skiplist的高度为3(因为除head_以外最高的节点"d"高度为3), 我们
 *  可以把leveldb中的skiplist看成一个简单的单链表，但是这个链表
 *  上的每个结点不单单只包含指向下一个结点的指针，可能包含很多个
 *  指向后续结点的指针，这样就可以快速的跳过一些不必要的结点，从
 *  而加快查找速度，对于链表内的结点拥有1 ~ kMaxHeight个指针，一
 *  结点拥有多少个指向后续元素的指针，这个过程是根据一个随机函数
 *  生成的
 *
 *  往skiplist中插入一个结点的大体步骤有三步
 *  我们假设当前skiplist的高度为cur_height
 *
 *  1. 在各层中(1 ~ cur_height)找到该结点插入位置的前一个结点的指针, 用一个
 *  零时的指针数组prev[kMaxHeight]进行记录(这步执行完毕之后prev[0 ~ cur_height-1]
 *  应该是指向各层级当中该结点插入位置的前一个结点的指针, 然后找到指向该结点插入位
 *  置后一个结点的指针(x), 如果插入位置后面没有结点了，那么这个指针指向的就是NULL.
 *
 *  2. 利用一个随机函数生成当前结点的高度random_height(这个高度不得高于kMaxHeight),
 *  此时如果random_height > cur_height, 那么令prev[cur_height ~ random_height - 1]
 *  分别指向head_.next_[cur_height ~ random_height - 1], (刚到达一个新的高度h时，该
 *  层肯定是没有任何数据结点的，所以前置结点肯定是head_.next[h], 而后置结点就是NULL)
 *  并且更新当前skiplist的高度cur_height = random_height(链表中最高的那个结点的高度, 就是skiplist的高度)
 *  此时如果random_height < cur_height，便什么也不用做, 因为prev[0 ~ random_height - 1]
 *  都已经被赋值了，也就是说各层中该结点插入位置的前置结点的位置都已经找到.
 *
 *  3. 生成高度为random_height的新结点(new_node), 然后修改指针指向就可以了，这里
 *  以第一层为例.
 *
 *  e.g..
 *    new_node指的是我们新生成的结点
 *    new_node.next_[0]指的是新节点第1层所指向的后继结点
 *    prev[0].next_[0]指的是当前新插入结点的前一个结点原先指向的后继结点
 *
 *    第一步我们要做得是令新结点的第一层的后继结点指针指向前一个结点原先指向的后继结点
 *    new_node.next_[0] = prev[0].next_[0]
 *    然后我们要做的就是令新节点作为前一个结点的后继结点
 *    prev[0].next_[0] = &new_node
 *
 */

template<typename Key, class Comparator>
void SkipList<Key,Comparator>::Insert(const Key& key) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  Node* prev[kMaxHeight];
  // x指向当前插入节点的后一个节点的地址，
  // 如果当前插入节点插入后已经是跳表最后一个节点，则x值为NULL
  Node* x = FindGreaterOrEqual(key, prev);

  // Our data structure does not allow duplicate insertion
  assert(x == NULL || !Equal(key, x->key));

  int height = RandomHeight();
  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {
      prev[i] = head_;
    }
    //fprintf(stderr, "Change height from %d to %d\n", max_height_, height);

    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (NULL), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since NULL sorts after all
    // keys.  In the latter case the reader will use the new node.
    max_height_.NoBarrier_Store(reinterpret_cast<void*>(height));
  }

  x = NewNode(key, height);
  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    prev[i]->SetNext(i, x);
  }
}

template<typename Key, class Comparator>
bool SkipList<Key,Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, NULL);
  if (x != NULL && Equal(key, x->key)) {
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_
