// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_H_

#include <cstddef>
#include <cstdint>

#include "leveldb/iterator.h"

namespace leveldb {

struct BlockContents;
class Comparator;

/**
 * @brief Block读取视图,用于从sstable中查看读取的Block数据
 * //! Block也是基于只读的思想，从文件中读取的sstable数据通过Slice，Iterator等手段进行访问共享的内存空间
 *
 */
class Block {
  public:
    // Initialize the block with the specified contents.
    explicit Block(const BlockContents& contents);

    Block(const Block&) = delete;
    Block& operator=(const Block&) = delete;

    ~Block();

    size_t size() const { return size_; }
    Iterator* NewIterator(const Comparator* comparator);      //* 通过唯一接口Iterator进行访问

  private:
    class Iter;

    uint32_t NumRestarts() const;

    const char* data_;
    size_t size_;
    uint32_t
        restart_offset_;  // Offset in data_ of restart array -- restart poing 数组开始偏移位置
    bool
        owned_;  // Block owns data_[] -- 是否拥有当前data数据(是否自己分配的堆内存空间)
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_H_
