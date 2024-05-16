// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_TWO_LEVEL_ITERATOR_H_
#define STORAGE_LEVELDB_TABLE_TWO_LEVEL_ITERATOR_H_

#include "leveldb/iterator.h"

namespace leveldb {

struct ReadOptions;

using SecondIterYieldFunction = Iterator* (*)(void*, const ReadOptions&, const Slice&);

//! 通过二级迭代器来访问sstable
// Return a new two level iterator.  A two-level iterator contains an
// index iterator whose values point to a sequence of blocks where
// each block is itself a sequence of key,value pairs.  The returned
// two-level iterator yields the concatenation of all key/value pairs
// in the sequence of blocks.  Takes ownership of "index_iter" and
// will delete it when no longer needed.
//? index iterator: 索引迭代器,指向一个block,每个data block包含了一系列的键值对
//? twoLevelIterator将一串data blocks连接起来,每个块内部又有一个迭代器可以遍历其内部的键值对
//? 双级迭代器的输出是所有块中键值对的concatenation(串联),将所有块中的键值对串联起来变成一个连续的序列,可以按顺序访问每一个键值对
//? 管理index_iter的所有权,在不需要时对其进行删除
//
// Uses a supplied function to convert an index_iter value into
// an iterator over the contents of the corresponding block.

/**
 * @brief 生成一个二级迭代器
 * //? 同样是为了隐藏实现细节，用NewTwoLevelIterator()替代new TwoLevelIterator()
 * //? 将TwoLevelIterator的实现放入cpp文件中
 *
 * @param index_iter : Iterator* index iterator,由index block得到
 * @param block_function : 通过index_entry的index_value来解析并得到data block iterator的函数指针
 * @param arg : void *  -- Table* 类型变量
 * @param options : const Options&
 * @return Iterator* 二级迭代器
 */
Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              SecondIterYieldFunction block_function, void* arg,
                              const ReadOptions& options);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_TWO_LEVEL_ITERATOR_H_
