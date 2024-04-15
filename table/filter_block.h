// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//* filterblockbuilder 生成一个字符串来存储filter block的内容
//* 最终结果存放在成员变量result_中，通过slice返回（注意builder的生命周期应与slice(result)一致）
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
/**
 * @brief filter block的结构如下所示
 * * filter data 1 *           -- 记录2^baselg(一般为2KB)大小的数据的filter信息，[base * k, base * (k+1)]范围内的数据
 * * filter data 2 *
 * * ...
 * * offset of filter data 1 * -- 记录filter data 1 的偏移量
 * * offset of filter data 2 *
 * * ...
 * * offset of (offset of filter data) -- 记录offset of filter data 的偏移量
 * * baselg (1 byte)
 */
class FilterBlockBuilder {
  public:
    explicit FilterBlockBuilder(const FilterPolicy*);

    FilterBlockBuilder(const FilterBlockBuilder&) = delete;
    FilterBlockBuilder& operator=(const FilterBlockBuilder&) = delete;

    void StartBlock(uint64_t block_offset); // 根据block offset定位一个编码滤波器的开始位置
    void AddKey(const Slice& key);
    Slice Finish();

  private:
    void GenerateFilter();

    const FilterPolicy* policy_;

    //* 用于记录一次filter生成的信息
    std::string keys_;             // Flattened key contents
    std::vector<size_t> start_;    // Starting index in keys_ of each key
    std::vector<Slice> tmp_keys_;  // policy_->CreateFilter() argument

    //* 用于记录生成的filter结果的信息
    std::string result_;                    // Filter data computed so far
    std::vector<uint32_t> filter_offsets_;  // 存放已经生成的滤波器偏移 为了生成block中的offset of filter data信息
};

class FilterBlockReader {
  public:
    // REQUIRES: "contents" and *policy must stay live while *this is live.
    FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
    bool KeyMayMatch(uint64_t block_offset, const Slice& key);

  private:
    const FilterPolicy* policy_;
    const char* data_;    // Pointer to filter data (at block-start) -- 数据data开始的位置
    const char* offset_;  // Pointer to beginning of offset array (at block-end) -- 偏移量offset of filter data开始的位置
    size_t num_;          // Number of entries in offset array -- filter data块的数量
    size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file)
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
