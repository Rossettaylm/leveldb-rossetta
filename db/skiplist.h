// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// * 写需要外部的同步机制，如mutex互斥锁，防止同时对skiplist进行修改
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
// * 读操作需要保证读进程运行时skiplist不被销毁；同时读取操作本身不需要内部的锁或者同步机制
//
// Invariants: // * 不变性 -- 跳表不提供删除接口，即不删除跳表节点，而是通过插入type == delete的节点进行删除
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
// * 分配的节点在跳表销毁前不会被删除。由于代码中从不删除任何跳表节点，这一点很容易得到保证。
// * 这意味着一旦节点被创建并加入跳表，它就会一直存在，直到跳表本身被销毁。
// ! 为了保证对跳表读取时不进行加锁
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
// * 节点的内容（除了指向下一个/前一个节点的指针）在节点加入跳表后是不可变的。
// * 这意味着一旦节点被插入到跳表中，它存储的数据就不会改变。
// * 只有插入操作（Insert）会修改列表，而且在初始化节点和使用释放-存储（release-stores）来发布节点到一个或多个列表时，都会非常小心。
// ! 通过牺牲一定的操作来保证不变性，从而保证线程安全
//
// ... prev vs. next pointer ordering ...

#include <atomic>
#include <cassert>
#include <cstdlib>

#include "util/arena.h"
#include "util/random.h"

namespace leveldb {

template <typename Key, class Comparator>
class SkipList {
  private:
    struct Node;

  public:
    // Create a new SkipList object that will use "cmp" for comparing keys,
    // and will allocate memory using "*arena".  Objects allocated in the arena
    // must remain allocated for the lifetime of the skiplist object.
    explicit SkipList(Comparator cmp, Arena* arena);

    SkipList(const SkipList&) = delete;
    SkipList& operator=(const SkipList&) = delete;

    // Insert key into the list.
    // REQUIRES: nothing that compares equal to key is currently in the list.
    //? 插入
    void Insert(const Key& key);

    // Returns true iff an entry that compares equal to key is in the list.
    //? 查找
    bool Contains(const Key& key) const;

    // Iteration over the contents of a skip list
    //? 通过迭代器进行遍历
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
        const SkipList* list_;  //* 迭代器所属的skiplist
        Node* node_;            //* 当前指向的节点
        // Intentionally copyable
    };

    // Iterator begin() {
    //     Iterator iter(this);
    //     iter.SeekToFirst();
    //     return iter;
    // }

  private:
    enum { kMaxHeight = 12 };

    inline int GetMaxHeight() const {
        //? 什么时候使用std::memory_order_relaxed？
        //? 其他的操作都是和max_height_原子变量地址无关的时候
        return max_height_.load(std::memory_order_relaxed);
    }

    Node* NewNode(const Key& key, int height);
    int RandomHeight();
    bool Equal(const Key& a, const Key& b) const {
        return (compare_(a, b) == 0);
    }

    // Return true if key is greater than the data stored in "n"
    bool KeyIsAfterNode(const Key& key, Node* n) const;

    // Return the earliest node that comes at or after key.
    //* Return nullptr if there is no such node.
    //
    // If prev is non-null, fills prev[level] with pointer to previous
    // node at "level" for every level in [0..max_height_-1].
    Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

    // Return the latest node with a key < key.
    //* Return head_ if there is no such node.
    Node* FindLessThan(const Key& key) const;

    // Return the last node in the list.
    // Return head_ if list is empty.
    Node* FindLast() const;

    // Immutable after construction
    Comparator const compare_;
    Arena* const arena_;  // Arena used for allocations of nodes

    Node* const head_;

    // Modified only by Insert().  Read racily by readers, but stale
    // values are ok.
    std::atomic<int>
        max_height_;  // Height of the entire list //* 用于多线程维护整个skiplist的最大层高

    // Read/written only by Insert().
    Random rnd_;
};

// Implementation details follow
/**
 * @brief Node的实现细节
 * @tparam Key
 * @tparam Comparator 比较器
 */
template <typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node {
    explicit Node(const Key& k) : key(k) {}

    Key const key;

    // Accessors/mutators for links.  Wrapped in methods so we can
    // add the appropriate barriers as necessary.
    //! 通过原子变量和内存屏障的方式实现了无锁队列
    Node* Next(int n) {
        assert(n >= 0);
        // Use an 'acquire load' so that we observe a fully initialized
        // version of the returned Node.
        //? 读保证，后续的读操作不能出现在取Next节点之前。保证对Next节点访问的内存一致性
        // 通俗来说，如果之后的读操作依赖于先对Next节点进行读取，则需要保证之后的read不被重排到读取Next节点之前
        return next_[n].load(std::memory_order_acquire);
    }

    void SetNext(int n, Node* x) {
        assert(n >= 0);
        // Use a 'release store' so that anybody who reads through this
        // pointer observes a fully initialized version of the inserted node.
        //? 写保证，之前的写操作不能出现在修改Next节点之后。保证对Next节点修改的内存一致性
        // 通俗来说，如果对Next节点的更改依赖于之前的某个写操作，则需要对其写顺序进行重排限制
        next_[n].store(x, std::memory_order_release);
    }

    // No-barrier variants that can be safely used in a few locations.
    Node* NoBarrier_Next(int n) {
        assert(n >= 0);
        return next_[n].load(std::memory_order_relaxed);
    }
    void NoBarrier_SetNext(int n, Node* x) {
        assert(n >= 0);
        next_[n].store(x, std::memory_order_relaxed);
    }

  private:
    // Array of length equal to the node height.  next_[0] is lowest level link.
    //? 只分配了一层的next指针，其余通过NewNode函数在堆上分配连续内存来存放，需要配合arena来管理内存
    //! 动态指定层高的一种方法
    std::atomic<Node*> next_[1];
};

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::NewNode(
    const Key& key, int height) {
    //* new node的内存分配，其中Node中有一层，再分配(height-1)*sizeof(std::atomic<Node*>)来存储其余层的指针
    char* const node_memory = arena_->AllocateAligned(
        sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));

    // placement new
    return new (node_memory) Node(key);
}

template <typename Key, class Comparator>
inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list) {
    list_ = list;
    node_ = nullptr;
}

template <typename Key, class Comparator>
inline bool SkipList<Key, Comparator>::Iterator::Valid() const {
    return node_ != nullptr;
}

template <typename Key, class Comparator>
inline const Key& SkipList<Key, Comparator>::Iterator::key() const {
    assert(Valid());
    return node_->key;
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Next() {
    assert(Valid());
    node_ = node_->Next(0);  //* 最底层的下一个节点为相邻节点
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Prev() {
    // Instead of using explicit "prev" links, we just search for the
    // last node that falls before key.
    assert(Valid());
    node_ = list_->FindLessThan(node_->key);
    if (node_ == list_->head_) {
        node_ = nullptr;
    }
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
    //* prev传入nullptr参数,代表不需要再target之前进行插入,则不需要找到其prev节点
    node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToFirst() {
    node_ = list_->head_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToLast() {
    node_ = list_->FindLast();
    if (node_ == list_->head_) {
        node_ = nullptr;
    }
}

template <typename Key, class Comparator>
int SkipList<Key, Comparator>::RandomHeight() {
    // Increase height with probability 1 in kBranching
    static const unsigned int kBranching = 4;
    int height = 1;
    //* 有1/kBranching的概率增加层高
    while (height < kMaxHeight && rnd_.OneIn(kBranching)) {
        height++;
    }
    assert(height > 0);
    assert(height <= kMaxHeight);
    return height;
}

/**
 * @brief 判断key是否大于Node，即afterNode
 *
 * @tparam Key
 * @tparam Comparator
 * @param key
 * @param n
 * @return true
 * @return false
 */
template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
    // null n is considered infinite
    return (n != nullptr) && (compare_(n->key, key) < 0);
}

/**
 * @brief 查找当前skiplist中大于等于key的节点
 *
 * @param key 查询的key值
 * @param prev 前驱结点的指针数组prev[kMaxHeight], Node **类型
 * @return SkipList<Key, Comparator>::Node*
 */
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key,
                                              Node** prev) const {
    Node* x = head_;
    int level = GetMaxHeight() - 1;  // 从最高层开始遍历，共n层，level = [0, n)

    // 从高层进行查询
    while (true) {
        //? 用x作为前驱，用next去和key进行比较，如果next满足大于等于key的条件，则返回next
        //? 否则重新从x开始，降一层重复查找
        //? 查找到了之后需要通过x节点维护prev变量
        Node* next = x->Next(level);
        //* 判断当前要查找的key是否在next节点之后，如果是则继续取下一个节点
        if (KeyIsAfterNode(key, next)) {
            // Keep searching in this list
            x = next;
        } else {
            //* key <= next->key 或者 next == nullptr
            // step1.如果有prev传进来，则维护prev数组
            if (prev != nullptr) prev[level] = x;

            // step2.如果在最低层（next一定是物理上跳跃间隔为1的node），则所寻找的node就是next
            if (level == 0) {
                return next;    // 返回的next是next->key >= key的位置，即查询或者待插入的位置
            } else {
                // Switch to next list
                // 降一层继续查找
                level--;
            }
        }
    }
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindLessThan(const Key& key) const {
    Node* x = head_;
    int level = GetMaxHeight() - 1;
    while (true) {
        assert(x == head_ || compare_(x->key, key) < 0);
        Node* next = x->Next(level);
        //? 判断条件:
        //? 1.  next == nullptr时,此时x为当前level末尾节点
        //? 2.  x->next->key >= key 时,此时x为当前level小于key的最大节点
        //? 3.  level == 0时到达最后一层
        if (next == nullptr || compare_(next->key, key) >= 0) {
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

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLast()
    const {
    Node* x = head_;
    int level = GetMaxHeight() - 1;
    while (true) {
        Node* next = x->Next(level);
        //? 判断条件: x->next == nullptr && level == 0时到达最后一个节点
        if (next == nullptr) {
            if (level == 0) {
                return x;  // 空列表时,x为head,此时返回head
            } else {
                // Switch to next list
                level--;
            }
        } else {
            x = next;
        }
    }
}

template <typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      //* 这里head_先初始化一个dummy node
      head_(NewNode(0 /* any key will do */, kMaxHeight)),
      max_height_(1),
      rnd_(0xdeadbeef) {
    for (int i = 0; i < kMaxHeight; i++) {
        head_->SetNext(i, nullptr);
    }
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key& key) {
    // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
    // here since Insert() is externally synchronized.
    //? step1. 查找插入位置，同时需要得到新节点的每一层前驱节点，等待后续的修改
    Node* prev[kMaxHeight];  //* 通过prev存储找到的x节点的每一层的前驱
    Node* x = FindGreaterOrEqual(key, prev);

    // Our data structure does not allow duplicate insertion
    // 此时x为key的待插入位置，x == nullptr时，插入队列的末尾; 跳表不允许插入同样的key
    //* 由于internal key的编码带有sequence number和type，所以同样的user key回产生不同的internal key，保证了跳表中没有相同的key值
    assert(x == nullptr || !Equal(key, x->key));

    //? step2. 为新节点生成随机层高，同时维护多出的层的前驱(用head指向新节点)
    int height = RandomHeight();  // 为当前将要插入的node生成随机层高

    //* 如果新节点的随机层高大于了目前最大层高，则对高层的prev指向为head
    //* 维护max_height_
    if (height > GetMaxHeight()) {
        for (int i = GetMaxHeight(); i < height; i++) {
            prev[i] = head_;
        }
        // It is ok to mutate max_height_ without any synchronization
        // with concurrent readers.  A concurrent reader that observes
        // the new value of max_height_ will see either the old value of
        // new level pointers from head_ (nullptr), or a new value set in
        // the loop below.  In the former case the reader will
        // immediately drop to the next level since nullptr sorts after all
        // keys.  In the latter case the reader will use the new node.
        //* max_height_在多线程环境下无须同步机制
        //* 如果max_height的新值被观察到，则会发生两种情况：旧的层级指针head_是空；新的值已经在下面循环中被设置
        //* 对nullptr的处理：nullptr被视为哨兵，即最大节点，此时下降一个层级进行查找，根据跳表的结构继续搜索。
        //* 使用新节点：如果观察到max_height的新值且新的节点层级指针被设置，则使用新值来加速查找。
        max_height_.store(height, std::memory_order_relaxed);
    }

    //? step3. 真正生成一个新的node并根据之前得到的prev数组，进行节点的修改
    x = NewNode(key, height);
    for (int i = 0; i < height; i++) {
        // NoBarrier_SetNext() suffices since we will add a barrier when
        // we publish a pointer to "x" in prev[i].
        //? 不保证操作的即使可见性，其余线程可能不能立即看见这个变化
        x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));

        //? 此时相当于将x插入了跳表中，需要使用内存屏障
        //? 添加一个内存屏障，保证更新next指针完成前，所有之前的内存写入操作对于后续都是可见的，即新节点的插入对于所有线程是可见的
        prev[i]->SetNext(i, x); // prev[i]至少是head_节点
    }
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::Contains(const Key& key) const {
    Node* x = FindGreaterOrEqual(key, nullptr);
    return x != nullptr && Equal(key, x->key);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_
