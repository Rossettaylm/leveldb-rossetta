// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/iterator.h"
#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"
#include "table/two_level_iterator.h"

namespace leveldb {

namespace {

// typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);
// using BlockFunction = Iterator* (*)(void*, const ReadOptions&, const Slice&);

/**
 * @brief 二级访问迭代器的实现方式：先通过index_iter查询所需要的key在data block序列中的位置，由BlockFunction提供的机制得到一个Block::iter，由此进行data block数据的遍历，
 * 注意需要在每次seek或者next/prev的时候注意是否跨越了data block的边界情况
 *
 */
class TwoLevelIterator : public Iterator {
  public:
    /**
    * @brief 构造一个二级迭代器用于遍历sstable的data blocks/遍历level files的file
    *
    * @param index_iter index block iterator
    * @param block_function 生成block_iterator的函数
    * @param arg : Table* 即sstable对象
    * @param options : ReadOptions 读取选项
    */
    TwoLevelIterator(Iterator* index_iter, SecondIterYieldFunction block_function,
                     void* arg, const ReadOptions& options);

    ~TwoLevelIterator() override;

    void Seek(const Slice& target) override;
    void SeekToFirst() override;
    void SeekToLast() override;
    void Next() override;
    void Prev() override;

    bool Valid() const override { return data_iter_.Valid(); }
    Slice key() const override {
        assert(Valid());
        return data_iter_.key();
    }
    Slice value() const override {
        assert(Valid());
        return data_iter_.value();
    }
    Status status() const override {
        // It'd be nice if status() returned a const Status& instead of a Status
        if (!index_iter_.status().ok()) {
            return index_iter_.status();
        } else if (data_iter_.iter() != nullptr && !data_iter_.status().ok()) {
            return data_iter_.status();
        } else {
            return status_;
        }
    }

  private:
    void SaveError(const Status& s) {
        if (status_.ok() && !s.ok()) status_ = s;
    }

    //* 用于two level iterator在不同的data block之间进行移动
    void SkipEmptyDataBlocksForward();
    void SkipEmptyDataBlocksBackward();

    //* 为two level iterator设置新的data block iterator
    void SetDataIterator(Iterator* data_iter);

    //* 根据index iter的key值来初始化一个data block iterator
    void InitDataBlock();

    SecondIterYieldFunction block_function_;
    void* arg_;  // Table *
    const ReadOptions options_;
    Status status_;
    IteratorWrapper index_iter_;
    IteratorWrapper data_iter_;  // May be nullptr
    // If data_iter_ is non-null, then "data_block_handle_" holds the
    // "index_value" passed to block_function_ to create the data_iter_.
    std::string data_block_handle_;
};

TwoLevelIterator::TwoLevelIterator(Iterator* index_iter,
                                   SecondIterYieldFunction block_function, void* arg,
                                   const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(nullptr) {}

TwoLevelIterator::~TwoLevelIterator() = default;

void TwoLevelIterator::Seek(const Slice& target) {
    index_iter_.Seek(target);
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
    SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst() {
    index_iter_.SeekToFirst();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
    SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
    index_iter_.SeekToLast();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
    SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
    assert(Valid());
    data_iter_.Next();
    SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
    assert(Valid());
    data_iter_.Prev();
    SkipEmptyDataBlocksBackward();
}

/**
 * @brief 当data iter为空或者已经超出data block范围时(!data_iter_.valid())，跳到下一个data block中
 *
 */
void TwoLevelIterator::SkipEmptyDataBlocksForward() {
    while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
        // Move to next block
        if (!index_iter_.Valid()) {
            SetDataIterator(nullptr);
            return;
        }
        index_iter_.Next();
        InitDataBlock();
        if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
    }
}

/**
 * @brief 当data iter为空或者已经超出data block范围时(!data_iter_.valid())，跳到上一个data block中
 *
 */
void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
    while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
        // Move to next block
        if (!index_iter_.Valid()) {
            SetDataIterator(nullptr);
            return;
        }
        index_iter_.Prev();
        InitDataBlock();
        if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
    }
}

/**
 * @brief 设置data iterator，需要保证旧的data iterator为空
 *
 * @param data_iter
 */
void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
    if (data_iter_.iter() != nullptr) SaveError(data_iter_.status());
    data_iter_.Set(data_iter);
}

void TwoLevelIterator::InitDataBlock() {
    //* 在index iter中没有查询到对应的key，则将data iter设置为nullptr
    if (!index_iter_.Valid()) {
        SetDataIterator(nullptr);
    } else {
        Slice handle = index_iter_.value();
        if (data_iter_.iter() != nullptr &&
            handle.compare(data_block_handle_) == 0) {
            //* data iter已经由此handle构造过了，不需要再进行重新构造
            // data_iter_ is already constructed with this iterator, so
            // no need to change anything
        } else {
            //* 构造一个新的data iter指向所需要查询的data block
            Iterator* iter = (*block_function_)(arg_, options_, handle);
            data_block_handle_.assign(handle.data(),
                                      handle.size());  // 更新data block handle
            SetDataIterator(iter);                     // 设置data iter
        }
    }
}

}  // namespace

Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              SecondIterYieldFunction block_function, void* arg,
                              const ReadOptions& options) {
    return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}  // namespace leveldb
