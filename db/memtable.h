// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_MEMTABLE_H_
#define STORAGE_LEVELDB_DB_MEMTABLE_H_

#include <string>

#include "db/dbformat.h"
#include "db/skiplist.h"
#include "leveldb/db.h"
#include "util/arena.h"

namespace leveldb {

class InternalKeyComparator;
class MemTableIterator;

/**
 * @brief MemTable存在于内存中，通过跳表构建，提供快速的插入查询功能
 * MemTable中存放着大量的键值对，键值对通过InternalKeyComparator进行排序
 * 其中InternalKey的结构为:
 * | uKey | sequence number (7 bytes) | type (1 byte) |
 *   key + 8 bytes
 */
class MemTable {
  public:
    // MemTables are reference counted.  The initial reference count
    // is zero and the caller must call Ref() at least once.
    //! 调用者需要通过Ref()和Unref()接口来使用memtable
    /**
     * @brief 通过InternalKeyComparator来构造一个MemTable
     *
     * @param comparator
     */
    explicit MemTable(const InternalKeyComparator& comparator);

    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;

    // Increase reference count.
    void Ref() { ++refs_; }

    // Drop reference count.  Delete if no more references exist.
    void Unref() {
        --refs_;
        assert(refs_ >= 0);
        // TODO(rossetta) 2024-04-07 21:11:12 学习通过引用计数来调用private析构的方法
        if (refs_ <= 0) {
            //* 显式的调用delete来进行private的析构
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
    /**
     * @brief 添加一个entry到memtable中
     *
     * @param seq 键值对序列号
     * @param type 类型value/deletion
     * @param key 键内容，user key --> internal key
     * @param value 值内容
     */
    void Add(SequenceNumber seq, ValueType type, const Slice& key,
             const Slice& value);

    // If memtable contains a value for key, store it in *value and return true.
    // If memtable contains a deletion for key, store a NotFound() error
    // in *status and return true.
    // Else, return false.
    bool Get(const LookupKey& key, std::string* value, Status* s);

  private:
    friend class MemTableIterator;
    friend class MemTableBackwardIterator;

    struct KeyComparator {
        const InternalKeyComparator comparator;
        explicit KeyComparator(const InternalKeyComparator& c)
            : comparator(c) {}
        int operator()(const char* a, const char* b) const;
    };

    typedef SkipList<const char*, KeyComparator> Table;

    //! 将析构函数设置为private的，通过引用计数Unref()来进行MemTable的析构
    ~MemTable();  // Private since only Unref() should be used to delete it

    KeyComparator comparator_;
    int refs_;
    Arena arena_;
    Table table_;  //* 跳表Table，用于存放键值对
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_
