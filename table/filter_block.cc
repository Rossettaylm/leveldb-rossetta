// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/filter_policy.h"
#include "table/filter_block.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
static constexpr size_t kFilterBaseLg = 11;  // 2^11 = 2KB
static constexpr size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

/**
 * @brief 从block offset开始的位置编码滤波器信息，如果block_offset大于0，则之前的所有数据留空的filter data
 *
 * @param block_offset
 */
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
    uint64_t filter_index = (block_offset / kFilterBase);
    assert(filter_index >= filter_offsets_.size());

    //* filter是按照kFilterBase大小来存放过滤器数据的，而传入的block_offset可能每次增加的内容由预设的options.block_size大小和添加的keys长度决定。此时需要根据根据filter_index循环添加filter data
    while (filter_index > filter_offsets_.size()) {
        GenerateFilter();
    }
}

/**
 * @brief 为当前的filter block添加一个key信息
 *
 * @param key
 */
void FilterBlockBuilder::AddKey(const Slice& key) {
    Slice k = key;
    start_.push_back(keys_.size());  // offset of current key
    keys_.append(k.data(), k.size());
}

/**
 * @brief 添加filter block的后缀部分，包括offset of filter data, offset of (offset of filter data), baselg
 *
 * @return Slice
 */
Slice FilterBlockBuilder::Finish() {
    if (!start_.empty()) {
        GenerateFilter();
    }

    // Append array of per-filter offsets
    //* 1. 添加offset of filter data部分
    const uint32_t array_offset = result_.size();
    for (size_t i = 0; i < filter_offsets_.size(); i++) {
        PutFixed32(&result_, filter_offsets_[i]);
    }

    //* 2. 添加offset of (offset of filter data)部分
    PutFixed32(&result_, array_offset);

    //* 3. 添加baselg部分
    result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
    return Slice(result_);
}

/**
 * @brief 从keys集合中,生成一个base的滤波器数据
 *
 */
void FilterBlockBuilder::GenerateFilter() {
    const size_t num_keys = start_.size();
    if (num_keys == 0) {
        // Fast path if there are no keys for this filter
        filter_offsets_.push_back(result_.size());
        return;
    }

    // Make list of keys from flattened key structure
    start_.push_back(keys_.size());  // Simplify length computation

    //* 将keys_中的所有key拆分成slice并存入tmp_keys_变量中
    tmp_keys_.resize(num_keys);
    for (size_t i = 0; i < num_keys; i++) {
        const char* base = keys_.data() + start_[i];
        size_t length = start_[i + 1] - start_[i];
        tmp_keys_[i] = Slice(base, length);
    }

    // Generate filter for current set of keys and append to result_.
    //* 为当前的keys_集合产生滤波器，并添加到result_中
    filter_offsets_.push_back(result_.size());

    //? CreateFilter接口中，keys为const Slice *，通过数组访问，vector<Slice>和const Slice *的内存顺序是一致的？
    policy_->CreateFilter(
        &tmp_keys_[0], static_cast<int>(num_keys),
        &result_);  // 通过bloom filter创建num_keys * bits_per_key长度的过滤信息，并append到result中

    tmp_keys_.clear();
    keys_.clear();
    start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
    size_t n = contents.size();
    if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array

    //* 1. 获取baselg
    base_lg_ = contents[n - 1];

    //* 2. 获取offset of (offset of filter data)
    uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
    if (last_word > n - 5) return;

    data_ = contents.data();         // filter data开始的位置
    offset_ = data_ + last_word;     // offset of filter data开始的位置
    num_ = (n - 5 - last_word) / 4;  // 计算filter data的总数量
}

/**
 * @brief 判断从block_offset开始的key数据是否能够通过滤波器
 *
 * @param block_offset
 * @param key
 * @return true
 * @return false
 */
bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
    uint64_t index =
        block_offset >>
        base_lg_;  // block_offset / base_lg，计算从第几个filter data块开始
    if (index < num_) {
        //* 解析filter data的偏移数据
        uint32_t start = DecodeFixed32(offset_ + index * 4);
        uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);

        if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
            Slice filter =
                Slice(data_ + start,
                      limit - start);  //* 获取第index个filter data块数据
            return policy_->KeyMayMatch(key, filter);  //* 进行key值匹配
        } else if (start == limit) {
            //* 此时表示filter为空
            // Empty filters do not match any keys
            return false;
        }
    }
    return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
