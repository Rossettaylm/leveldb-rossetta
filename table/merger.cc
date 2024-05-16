// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"
#include "table/merger.h"

namespace leveldb {

namespace {

/**
 * @brief 托管children iters(一般来自于一个version的sstable文件)同时提供一个返回的merge iter
 *
 */
class MergingIterator : public Iterator {
  public:
    MergingIterator(const Comparator* comparator, Iterator** children, int n)
        : comparator_(comparator),
          children_(new IteratorWrapper[n]),
          n_(n),
          current_(nullptr),
          direction_(kForward) {
        for (int i = 0; i < n; i++) {
            children_[i].Set(children[i]);
        }
    }

    //* 仅释放管理children iters的动态数组，不删除children iters本身
    ~MergingIterator() override { delete[] children_; }

    bool Valid() const override { return (current_ != nullptr); }

    void SeekToFirst() override {
        for (int i = 0; i < n_; i++) {
            children_[i].SeekToFirst();
        }
        FindSmallest();
        direction_ = kForward;
    }

    void SeekToLast() override {
        for (int i = 0; i < n_; i++) {
            children_[i].SeekToLast();
        }
        FindLargest();
        direction_ = kReverse;
    }

    void Seek(const Slice& target) override {
        for (int i = 0; i < n_; i++) {
            children_[i].Seek(
                target);  // 其中每个children iter都是一个 two level iter，通过index iter加速查找定位
        }
        FindSmallest();
        direction_ = kForward;
    }

    void Next() override {
        assert(Valid());

        // Ensure that all children are positioned after key().
        // If we are moving in the forward direction, it is already
        // true for all of the non-current_ children since current_ is
        // the smallest child and key() == current_->key().  Otherwise,
        // we explicitly position the non-current_ children.
        if (direction_ != kForward) {
            for (int i = 0; i < n_; i++) {
                IteratorWrapper* child = &children_[i];
                if (child != current_) {
                    child->Seek(key());
                    if (child->Valid() &&
                        comparator_->Compare(key(), child->key()) == 0) {
                        child->Next();
                    }
                }
            }
            direction_ = kForward;
        }

        current_->Next();
        FindSmallest();
    }

    void Prev() override {
        assert(Valid());

        // Ensure that all children are positioned before key().
        // If we are moving in the reverse direction, it is already
        // true for all of the non-current_ children since current_ is
        // the largest child and key() == current_->key().  Otherwise,
        // we explicitly position the non-current_ children.
        if (direction_ != kReverse) {
            for (int i = 0; i < n_; i++) {
                IteratorWrapper* child = &children_[i];
                if (child != current_) {
                    child->Seek(key());
                    if (child->Valid()) {
                        // Child is at first entry >= key().  Step back one to be < key()
                        child->Prev();
                    } else {
                        // Child has no entries >= key().  Position at last entry.
                        child->SeekToLast();
                    }
                }
            }
            direction_ = kReverse;
        }

        current_->Prev();
        FindLargest();
    }

    Slice key() const override {
        assert(Valid());
        return current_->key();
    }

    Slice value() const override {
        assert(Valid());
        return current_->value();
    }

    Status status() const override {
        Status status;
        for (int i = 0; i < n_; i++) {
            status = children_[i].status();
            if (!status.ok()) {
                break;
            }
        }
        return status;
    }

  private:
    // Which direction is the iterator moving?
    enum Direction { kForward, kReverse };

    void FindSmallest();
    void FindLargest();

    // We might want to use a heap in case there are lots of children.
    // For now we use a simple array since we expect a very small number
    // of children in leveldb.
    const Comparator* comparator_;
    IteratorWrapper* children_;
    int n_;
    IteratorWrapper* current_;
    Direction direction_;
};

/**
 * @brief 找到children iters中最小的迭代器，并用current指向它
 *
 */
void MergingIterator::FindSmallest() {
    IteratorWrapper* smallest = nullptr;
    for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child->Valid()) {
            if (smallest == nullptr) {
                smallest = child;
            } else if (comparator_->Compare(child->key(), smallest->key()) <
                       0) {
                smallest = child;
            }
        }
    }
    current_ = smallest;
}

/**
 * @brief 找到children iters中最大的迭代器，并用current指向它
 *
 */
void MergingIterator::FindLargest() {
    IteratorWrapper* largest = nullptr;
    for (int i = n_ - 1; i >= 0; i--) {
        IteratorWrapper* child = &children_[i];
        if (child->Valid()) {
            if (largest == nullptr) {
                largest = child;
            } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
                largest = child;
            }
        }
    }
    current_ = largest;
}
}  // namespace

/**
 * @brief //!提供给外部的接口
 *        通过children iters构造一个merge iter
 *
 * @param comparator
 * @param children
 * @param n
 * @return Iterator*
 */
Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children,
                             int n) {
    assert(n >= 0);
    if (n == 0) {
        return NewEmptyIterator();
    } else if (n == 1) {
        return children[0];
    } else {
        return new MergingIterator(comparator, children, n);
    }
}

}  // namespace leveldb
