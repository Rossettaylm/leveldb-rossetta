// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <list>

#include "leveldb/cache.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache::~Cache() {}

//! 匿名命名空间 -- 限制作用域，只能在当前cc文件中看见
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

//? 最近最少使用（LRU）缓存的实现

//? 缓存条目包含一个名为"in_cache"的布尔值，用来指示缓存是否对条目保持了引用。
//? 除了通过Erase()方法显式删除、插入一个具有重复键的元素时通过Insert()方法隐式删除，
//? 或者在缓存被销毁时，"in_cache"可能会在不通过"deleter"方法的情况下变为false。
//?
//? 缓存维护了两个链表来存储缓存中的项目。所有项目要么在一个链表中，要么在另一个链表中，但绝不会同时在两个链表中。
//? 被客户端引用但从缓存中删除的项目不会出现在任一链表中。这两个链表分别是：
//? - in-use（正在使用）: 包含当前被客户端引用的项目，顺序不定。（这个链表用于不变性检查。如果我们移除了这个检查，
//?   那些本应在这个链表上的元素可能会变成断开连接的单链表。）
//? - LRU（最近最少使用）: 包含当前没有被客户端引用的项目，按照LRU顺序排列。
//? 元素通过Ref()和Unref()方法在这些链表之间移动，这些方法在检测到元素在缓存中获取或失去其唯一的
//? 外部引用时会进行操作。

//? 一个条目是一个可变长度的堆分配结构。条目被保持在一个循环双向链表中，按照访问时间排序。

struct LRUHandle {
    void* value;
    void (*deleter)(const Slice&, void* value);
    LRUHandle* next_hash;  // handletable中哈希链表的next指针

    // 用于维护LRUHandle链表的双链表指针 lru_list / in_use_list
    LRUHandle* next;
    LRUHandle* prev;

    size_t charge;  // TODO(opt): Only allow uint32_t?
    size_t key_length;
    bool in_cache;  // Whether entry is in the cache.
    uint32_t refs;  // References, including cache reference, if present.
    uint32_t hash;  // Hash of key(); used for fast sharding and comparisons
    char key_data
        [1];  // Beginning of key      只存key值开始的元素，其余分配在堆上，节省handle的空间 malloc(sizeof(LRUHandle) - 1 + key.size())，且key_data必须在最后

    Slice key() const {
        // next is only equal to this if the LRU handle is the list head of an
        // empty list. List heads never have meaningful keys.
        assert(next != this);

        return Slice(key_data, key_length);
    }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
/**
 * @brief LRUHandle哈希表实现
 */
class HandleTable {
  public:
    HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
    ~HandleTable() { delete[] list_; }

    /**
     * @brief 查询
     *
     * @param key
     * @param hash
     * @return LRUHandle*
     */
    LRUHandle* Lookup(const Slice& key, uint32_t hash) {
        return *FindPointer(key, hash);
    }

    /**
     * @brief 插入LRUHandle并返回旧的handle节点
     *
     * @param h
     * @return LRUHandle* 旧的handle节点或者nullptr
     */
    LRUHandle* Insert(LRUHandle* h) {
        //* step1. 寻找当前table中是否有同样key和hash的LRUHandle*节点，如果存在则进行替换
        LRUHandle** ptr = FindPointer(h->key(), h->hash);
        LRUHandle* old = *ptr;
        h->next_hash = (old == nullptr ? nullptr : old->next_hash);
        *ptr = h;  // 替换成新的

        // old == nullptr时表明未搜寻到同样的LRUHandle，更新elem个数并判断是否需要rehash
        if (old == nullptr) {
            ++elems_;
            if (elems_ > length_) {
                // Since each cache entry is fairly large, we aim for a small
                // average linked list length (<= 1).
                Resize();
            }
        }
        return old;
    }

    /**
     * @brief 从table中移除一个节点，并返回已移除的节点
     *
     * @param key
     * @param hash
     * @return LRUHandle* 移除的节点或者nullptr
     */
    LRUHandle* Remove(const Slice& key, uint32_t hash) {
        LRUHandle** ptr = FindPointer(key, hash);
        LRUHandle* result = *ptr;
        if (result != nullptr) {
            *ptr = result->next_hash;  // 指向下一个节点
            --elems_;
        }
        return result;
    }

  private:
    // The table consists of an array of buckets where each bucket is
    // a linked list of cache entries that hash into the bucket.
    //? LRUHandle哈希表底层是buckets数组，其中每个bucket是存放LRUHandle的链表
    uint32_t length_;   // buckets array的长度
    uint32_t elems_;    // handle elems的总个数
    LRUHandle** list_;  // buckets array

    // Return a pointer to slot that points to a cache entry that
    // matches key/hash.  If there is no such cache entry, return a
    // pointer to the trailing slot in the corresponding linked list.
    /**
     * @brief 寻找table中是否存在对应key和hash的节点，如果存在返回节点，不存在则返回链表末尾的指针(待插入位置)
     *
     * @param key
     * @param hash
     * @return LRUHandle** node位置的指针，可能返回的是待插入位置(next_hash)，方便进行修改
     */
    LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
        LRUHandle** ptr = &list_[hash & (length_ - 1)];

        // 遍历hash链表直到找到当前key和hash确定的LRUHandle*
        while (*ptr != nullptr &&
               ((*ptr)->hash != hash || key != (*ptr)->key())) {
            ptr = &(*ptr)->next_hash;
        }
        return ptr;
    }

    /**
     * @brief rehash整个哈希链表
     *
     */
    void Resize() {
        //* step1. 以倍增的方式分配新的buckets array长度，保证大于总的元素个数
        uint32_t new_length = 4;
        while (new_length < elems_) {
            new_length *= 2;
        }

        //* step2. 分配buckets array空间，其中每个LRUHandle* 代表了一个链表
        LRUHandle** new_list = new LRUHandle*[new_length];
        memset(new_list, 0, sizeof(new_list[0]) * new_length);

        uint32_t count = 0;
        for (uint32_t i = 0; i < length_; i++) {
            LRUHandle* h = list_[i];  // 遍历旧buckets array，h为链表头节点
            while (h != nullptr) {
                LRUHandle* next = h->next_hash;
                uint32_t hash = h->hash;
                LRUHandle** ptr =
                    &new_list[hash &
                              (new_length -
                               1)];  // rehash一下，指定分配到新的hash链表中

                h->next_hash = *ptr;  // 将h节点挂到新链表的最前端
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
//? sharded cache指一个cache系统的片段，用于分布式缓存系统的设计，当一个类被定义为分片类时，其实例将分布在分片集群中任何已定义的数据节点上
class LRUCache {
  public:
    LRUCache();
    ~LRUCache();

    // Separate from constructor so caller can easily make an array of LRUCache
    void SetCapacity(size_t capacity) { capacity_ = capacity; }

    // Like Cache methods, but with an extra "hash" parameter.
    Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value,
                          size_t charge,
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
    void LRU_Append(LRUHandle* list, LRUHandle* e);
    void Ref(LRUHandle* e);
    void Unref(LRUHandle* e);
    bool FinishErase(LRUHandle* e) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    // Initialized before use.
    size_t capacity_;

    // mutex_ protects the following state.
    mutable port::Mutex mutex_;
    size_t usage_ GUARDED_BY(mutex_);

    // Dummy head of LRU list.
    // lru.prev is newest entry, lru.next is oldest entry.
    // Entries have refs==1 and in_cache==true.
    //? lru.prev代表最新的entry，lru.next代表最旧的entry (循环双链表)，同时，最新的handle被存放在链表尾部，可通过lru.prev进行访问
    //? lru list中的节点必有handle->in_cache == true && handle->refs == 1，代表其在cache中且没有被使用
    LRUHandle lru_ GUARDED_BY(
        mutex_);  // lru链表dummy head，通过lru算法维护并存放cache中的handle

    // Dummy head of in-use list.
    // Entries are in use by clients, and have refs >= 2 and in_cache==true.
    //? 循环双链表（同上）
    //? in_use_list中的节点必有handle->refs >= 2 && handle->in_cache == true
    LRUHandle in_use_ GUARDED_BY(mutex_);

    HandleTable table_ GUARDED_BY(
        mutex_);  //! LRU通过双链表加哈希表进行实现，即哈希表为了解决双链表查询速度慢的问题
};

LRUCache::LRUCache() : capacity_(0), usage_(0) {
    // Make empty circular linked lists.
    lru_.next = &lru_;
    lru_.prev = &lru_;
    in_use_.next = &in_use_;
    in_use_.prev = &in_use_;
}

LRUCache::~LRUCache() {
    //* 析构时保证in use列表为空
    assert(in_use_.next ==
           &in_use_);  // Error if caller has an unreleased handle
    for (LRUHandle* e = lru_.next; e != &lru_;) {
        LRUHandle* next = e->next;
        assert(e->in_cache);
        e->in_cache = false;
        assert(e->refs == 1);  // Invariant of lru_ list.
        // 保证lru list中的所有handle都是in_cache == true && refs == 1
        Unref(e);
        e = next;
    }
}

void LRUCache::Ref(LRUHandle* e) {
    if (e->refs == 1 &&
        e->in_cache) {  // If on lru_ list, move to in_use_ list.
        LRU_Remove(e);
        LRU_Append(&in_use_, e);
    }
    e->refs++;
}

void LRUCache::Unref(LRUHandle* e) {
    assert(e->refs > 0);

    e->refs--;  // 先将其引用计数减1

    if (e->refs == 0) {  // Deallocate.
        //* 情况1：从lru list中彻底删除并释放空间
        assert(!e->in_cache);
        (*e->deleter)(e->key(), e->value);
        free(e);
    } else if (e->in_cache && e->refs == 1) {
        //* 情况2：从in use list中删除并移动到lru list中
        // No longer in use; move to lru_ list.
        LRU_Remove(e);
        LRU_Append(&lru_, e);
    }
}

/**
 * @brief 双链表删除
 * @param e
 */
void LRUCache::LRU_Remove(LRUHandle* e) {
    e->next->prev = e->prev;
    e->prev->next = e->next;
}

/**
 * @brief 双链表添加到末尾(lru最新节点)
 *
 * @param list 链表
 * @param e handle节点
 */
void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
    // Make "e" newest entry by inserting just before *list
    // 将e节点放入list的尾部，并维护循环双链表
    e->next = list;
    e->prev = list->prev;
    e->prev->next = e;
    e->next->prev = e;
}

/**
 * @brief 从table中查找entry并返回,通过mutex保护 !! 通过哈希表解决了对于双链表的查询速度慢的问题
 *
 * @param key
 * @param hash
 * @return Cache::Handle* 返回的handle进行了一次ref，不使用时需要配合release函数进行unref
 */
Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
    MutexLock l(&mutex_);
    LRUHandle* e = table_.Lookup(key, hash);
    if (e != nullptr) {
        Ref(e);
    }
    return reinterpret_cast<Cache::Handle*>(e);
}

/**
 * @brief 释放对于handle的使用，即执行一次unref
 *
 * @param handle
 */
void LRUCache::Release(Cache::Handle* handle) {
    MutexLock l(&mutex_);
    Unref(reinterpret_cast<LRUHandle*>(handle));
}

Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value,
                                size_t charge,
                                void (*deleter)(const Slice& key,
                                                void* value)) {
    MutexLock l(&mutex_);

    //* step1. 从entry的key和hash和value创建一个LRUHandle句柄 (malloc)
    LRUHandle* e = reinterpret_cast<LRUHandle*>(malloc(
        sizeof(LRUHandle) - 1 + key.size()));  // malloc可以更加灵活的指定空间
    e->value = value;
    e->deleter = deleter;
    e->charge = charge;
    e->key_length = key.size();
    e->hash = hash;
    e->in_cache = false;
    e->refs = 1;  // for the returned handle.
    std::memcpy(e->key_data, key.data(),
                key.size());  // 将key值拷贝到key_data空间

    if (capacity_ > 0) {
        //* step2. 将新插入的entry节点放到in use list和table中，即新节点handle立马投入使用状态
        e->refs++;  // for the cache's reference.
        e->in_cache = true;
        LRU_Append(&in_use_, e);  // 插入到in use list
        usage_ += charge;
        FinishErase(
            table_.Insert(e));  // 插入到table中，并将返回的旧节点从cache中移除
    } else {  // don't cache. (capacity_==0 is supported and turns off caching.)
        // next is read by key() in an assert, so it must be initialized
        //* capacity == 0，此时不启用cache
        e->next =
            nullptr;  // 为了使用LRUHandle->key()方法内的assert(next != nullptr)，需要对其next进行初始化
    }

    //* step3. 判断是否cache容量不够，是则删除最老的节点
    // lru_.next == &lru_时表示空队列
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

// If e != nullptr, finish removing *e from the cache; it has already been
// removed from the hash table.  Return whether e != nullptr.
/**
 * @brief 将节点e从cache中移除，并返回e是否为空 !!注意，此函数配合table.insert()的返回值使用，即将从table中返回的旧节点从cache中移除
 *
 * @param e
 * @return true
 * @return false
 */
bool LRUCache::FinishErase(LRUHandle* e) {
    if (e != nullptr) {
        assert(e->in_cache);
        LRU_Remove(e);
        e->in_cache = false;
        usage_ -= e->charge;
        Unref(
            e);  // 从cache中移除后但是如果refs > 1，即处于in use状态时，会保留空间直到ref减为0
    }
    return e != nullptr;
}

/**
 * @brief 删除一个节点
 *
 * @param key
 * @param hash
 */
void LRUCache::Erase(const Slice& key, uint32_t hash) {
    MutexLock l(&mutex_);
    FinishErase(table_.Remove(key, hash));
}

/**
 * @brief 删除所有未被使用(lru list中的)的entry，用于控制内存的使用不超出限制
 *
 */
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
static const int kNumShards = 1 << kNumShardBits;  // 2^4 = 16

/**
 * @brief LRUCache的分布式实现，内部共存放了kNumShards个片段，通过hash的低kNumShardBits位来确定新数据放置在那个片段中
 */
class ShardedLRUCache : public Cache {
  private:
    LRUCache shard_[kNumShards];  // 16个LRUcache分布
    port::Mutex id_mutex_;
    uint64_t last_id_;  // 每有一个新用户，则需要获取一个新id

    static inline uint32_t HashSlice(const Slice& s) {
        return Hash(s.data(), s.size(), 0);
    }

    /**
    * @brief 取哈希值hash的低kNumShardBits位作为shard值，指定分配到哪个cache片段中
    *
    * @param hash
    * @return uint32_t
    */
    static uint32_t Shard(uint32_t hash) {
        return hash >> (32 - kNumShardBits);
    }

  public:
    explicit ShardedLRUCache(size_t capacity) : last_id_(0) {
        const size_t per_shard = (capacity + (kNumShards - 1)) /
                                 kNumShards;  // 将cap平均分配给不同的片段
        for (int s = 0; s < kNumShards; s++) {
            shard_[s].SetCapacity(per_shard);
        }
    }
    ~ShardedLRUCache() override {}
    Handle* Insert(const Slice& key, void* value, size_t charge,
                   void (*deleter)(const Slice& key, void* value)) override {
        const uint32_t hash = HashSlice(key);  // 根据key生成hash值
        return shard_[Shard(hash)].Insert(key, hash, value, charge,
                                          deleter);  // 分配到某个LRUCache分段中
    }
    Handle* Lookup(const Slice& key) override {
        const uint32_t hash = HashSlice(key);
        return shard_[Shard(hash)].Lookup(key, hash);
    }
    void Release(Handle* handle) override {
        LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
        shard_[Shard(h->hash)].Release(handle);
    }
    void Erase(const Slice& key) override {
        const uint32_t hash = HashSlice(key);
        shard_[Shard(hash)].Erase(key, hash);
    }
    void* Value(Handle* handle) override {
        return reinterpret_cast<LRUHandle*>(handle)->value;
    }
    uint64_t NewId() override {
        MutexLock l(&id_mutex_);
        return ++(last_id_);
    }
    void Prune() override {
        for (int s = 0; s < kNumShards; s++) {
            shard_[s].Prune();
        }
    }
    size_t TotalCharge() const override {
        size_t total = 0;
        for (int s = 0; s < kNumShards; s++) {
            total += shard_[s].TotalCharge();
        }
        return total;
    }
};

}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) { return new ShardedLRUCache(capacity); }

}  // namespace leveldb
