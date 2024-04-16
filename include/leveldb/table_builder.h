// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// TableBuilder provides the interface used to build a Table
// (an immutable and sorted map from keys to values).
//
// Multiple threads can invoke const methods on a TableBuilder without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same TableBuilder must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_

#include <cstdint>

#include "leveldb/export.h"
#include "leveldb/options.h"
#include "leveldb/status.h"

namespace leveldb {

class BlockBuilder;
class BlockHandle;
class WritableFile;

/**
 * @brief 用于构建sstable
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
//? blockhandle的复用：对于索引block，其entry的value部分都是复用了blockhandle进行数据block的定位

class LEVELDB_EXPORT TableBuilder {
  public:
    // Create a builder that will store the contents of the table it is
    // building in *file.  Does not close the file.  It is up to the
    // caller to close the file after calling Finish().
    TableBuilder(const Options& options, WritableFile* file);

    TableBuilder(const TableBuilder&) = delete;
    TableBuilder& operator=(const TableBuilder&) = delete;

    // REQUIRES: Either Finish() or Abandon() has been called.
    ~TableBuilder();

    // Change the options used by this builder.  Note: only some of the
    // option fields can be changed after construction.  If a field is
    // not allowed to change dynamically and its value in the structure
    // passed to the constructor is different from its value in the
    // structure passed to this method, this method will return an error
    // without changing any fields.
    Status ChangeOptions(const Options& options);

    // Add key,value to the table being constructed.
    // REQUIRES: key is after any previously added key according to comparator.
    // REQUIRES: Finish(), Abandon() have not been called
    /**
     * @brief 通过add按递增序将键值对写入到sstable中的data block中,如果超过了设定的options.block_size,则调用Flush()
     *
     * @param key
     * @param value
     */
    void Add(const Slice& key, const Slice& value);

    // Advanced operation: flush any buffered key/value pairs to file.
    // Can be used to ensure that two adjacent entries never live in
    // the same data block.  Most clients should not need to use this method.
    // REQUIRES: Finish(), Abandon() have not been called
    /**
     * @brief 将data_block的内容flush到文件中,并更新对应的index_entry和filter block
     */
    void Flush();

    // Return non-ok iff some error has been detected.
    Status status() const;

    // Finish building the table.  Stops using the file passed to the
    // constructor after this function returns.
    // REQUIRES: Finish(), Abandon() have not been called
    /**
     * @brief 将除data block外的元数据块写入到sstable中,完成一个sstable
     * @return Status
     */
    Status Finish();

    // Indicate that the contents of this builder should be abandoned.  Stops
    // using the file passed to the constructor after this function returns.
    // If the caller is not going to call Finish(), it must call Abandon()
    // before destroying this builder.
    // REQUIRES: Finish(), Abandon() have not been called
    void Abandon();

    // Number of calls to Add() so far.
    uint64_t NumEntries() const;

    // Size of the file generated so far.  If invoked after a successful
    // Finish() call, returns the size of the final generated file.
    uint64_t FileSize() const;

  private:
    bool ok() const { return status().ok(); }
    void WriteBlock(BlockBuilder* block, BlockHandle* handle);
    void WriteRawBlock(const Slice& data, CompressionType, BlockHandle* handle);

    //! 将table_builder的成员变量再封装一层，起到了封装性的作用
    struct Rep;
    Rep* rep_;  //* table中的内容构建成员表示结构体
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_
