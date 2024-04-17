// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin cache implementation with a least-recently-used eviction
// policy is provided.  Clients may use their own implementations if
// they want something more sophisticated (like scan-resistance, a
// custom eviction policy, variable cache sizing, etc.)
//? 1. Cache是一个键值对映射集合的接口实现
//? 2. 提供了同步行以保证并发访问时的线程安全
//? 3. Cache具有驱逐策略(eviction policy)来保证用新数据动态替换旧的数据
//? 4. 每次插入Cache的数据需要一定的charge来消耗Cache的容量
//? 5. 内建的Cache为LRUCache，实现采用了LRU算法作为eviction policy，同时用户可根据需求按接口实现自己的Cache

#ifndef STORAGE_LEVELDB_INCLUDE_CACHE_H_
#define STORAGE_LEVELDB_INCLUDE_CACHE_H_

#include <cstdint>

#include "leveldb/export.h"
#include "leveldb/slice.h"

namespace leveldb {

class LEVELDB_EXPORT Cache;

// Create a new cache with a fixed size capacity.  This implementation
// of Cache uses a least-recently-used eviction policy.
LEVELDB_EXPORT Cache* NewLRUCache(size_t capacity);

class LEVELDB_EXPORT Cache {
  public:
    Cache() = default;

    Cache(const Cache&) = delete;
    Cache& operator=(const Cache&) = delete;

    // Destroys all existing entries by calling the "deleter"
    // function that was passed to the constructor.
    virtual ~Cache();

    // Opaque handle to an entry stored in the cache.
    //? Handle是指向存放在Cache中的一个entry的句柄
    struct Handle {};

    // Insert a mapping from key->value into the cache and assign it
    // the specified charge against the total cache capacity.
    //
    // Returns a handle that corresponds to the mapping.  The caller
    // must call this->Release(handle) when the returned mapping is no
    // longer needed.
    //
    // When the inserted entry is no longer needed, the key and
    // value will be passed to "deleter".
    //? 插入一个键值对并返回handle，当调用者不再需要handle时，需要调用Release(handle)释放句柄
    //? 同时每个entry需要传入一个deleter，当entry不再被需要时，会调用deleter函数删除这一entry
    virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                           void (*deleter)(const Slice& key, void* value)) = 0;

    // If the cache has no mapping for "key", returns nullptr.
    //
    // Else return a handle that corresponds to the mapping.  The caller
    // must call this->Release(handle) when the returned mapping is no
    // longer needed.
    virtual Handle* Lookup(const Slice& key) = 0;

    // Release a mapping returned by a previous Lookup().
    // REQUIRES: handle must not have been released yet.
    // REQUIRES: handle must have been returned by a method on *this.
    virtual void Release(Handle* handle) = 0;

    // Return the value encapsulated in a handle returned by a
    // successful Lookup().
    // REQUIRES: handle must not have been released yet.
    // REQUIRES: handle must have been returned by a method on *this.
    virtual void* Value(Handle* handle) = 0;

    // If the cache contains entry for key, erase it.  Note that the
    // underlying entry will be kept around until all existing handles
    // to it have been released.
    //? erase一个entry，此entry会被保留直到与其相关的handle都被释放
    virtual void Erase(const Slice& key) = 0;

    // Return a new numeric id.  May be used by multiple clients who are
    // sharing the same cache to partition the key space.  Typically the
    // client will allocate a new id at startup and prepend the id to
    // its cache keys.
    //? 当多个用户共享此cache空间时，需要分配一个new id进行区分
    virtual uint64_t NewId() = 0;

    // Remove all cache entries that are not actively in use.  Memory-constrained
    // applications may wish to call this method to reduce memory usage.
    // Default implementation of Prune() does nothing.  Subclasses are strongly
    // encouraged to override the default implementation.  A future release of
    // leveldb may change Prune() to a pure abstract method.
    //? 修剪，删除；删除所有未被使用(lru list中的)的entry，用于控制内存的使用不超出限制；默认无具体实现，未来的leveldb可能将其改为纯虚函数
    virtual void Prune() {}

    // Return an estimate of the combined charges of all elements stored in the
    // cache.
    virtual size_t TotalCharge() const = 0;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_CACHE_H_
