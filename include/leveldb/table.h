// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_H_

#include <cstdint>

#include "leveldb/export.h"
#include "leveldb/iterator.h"

namespace leveldb {

class Block;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
/**
 * @brief table是一个有序的map<string, string>，且不可变和持久化的，一个table可以被多线程安全访问而不需要同步机制
 *
 */

//* data block 1    --
//* data block 2    --
//* data block 3    --> 数据部分
//* data block 4    --
//* data block 5    --
//* filter block    --> 元数据部分
//* meta index block --> 元数据的索引 filtername + filter block handle
//* index block     --> 数据的索引 entry: maxkey + data block handle
//* footer          --> 索引的索引 meta index block handle + index block handle

class LEVELDB_EXPORT Table {
  public:
    // Attempt to open the table that is stored in bytes [0..file_size)
    // of "file", and read the metadata entries necessary to allow
    // retrieving data from the table.
    //
    // If successful, returns ok and sets "*table" to the newly opened
    // table.  The client should delete "*table" when no longer needed.
    // If there was an error while initializing the table, sets "*table"
    // to nullptr and returns a non-ok status.  Does not take ownership of
    // "*source", but the client must ensure that "source" remains live
    // for the duration of the returned table's lifetime.
    //
    // *file must remain live while this Table is in use.
    static Status Open(const Options& options, RandomAccessFile* file,
                       uint64_t file_size, Table** table);

    Table(const Table&) = delete;
    Table& operator=(const Table&) = delete;

    ~Table();

    // Returns a new iterator over the table contents.
    // The result of NewIterator() is initially invalid (caller must
    // call one of the Seek methods on the iterator before using it).
    Iterator* NewIterator(const ReadOptions&) const;

    // Given a key, return an approximate byte offset in the file where
    // the data for that key begins (or would begin if the key were
    // present in the file).  The returned value is in terms of file
    // bytes, and so includes effects like compression of the underlying data.
    // E.g., the approximate offset of the last key in the table will
    // be close to the file length.
    //* 给定一个key值，计算其在sstable文件中的大概偏移量
    uint64_t ApproximateOffsetOf(const Slice& key) const;

  private:
    friend class TableCache;
    struct Rep;

    static Iterator* BlockReader(void*, const ReadOptions&, const Slice&);

    explicit Table(Rep* rep) : rep_(rep) {}

    // Calls (*handle_result)(arg, ...) with the entry found after a call
    // to Seek(key).  May not make such a call if filter policy says
    // that key is not present.
    /**
     * @brief 调用handle_result来处理block内部通过seek得到的entry
     *
     * @param key
     * @param arg
     * @param handle_result
     * @return Status
     */
    Status InternalGet(const ReadOptions&, const Slice& key, void* arg,
                       void (*handle_result)(void* arg, const Slice& k,
                                             const Slice& v));

    void ReadMeta(const Footer& footer);
    void ReadFilter(const Slice& filter_handle_value);

    //! 隐藏Table成员变量和实现细节
    Rep* const rep_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_H_
