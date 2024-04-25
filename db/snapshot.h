// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SNAPSHOT_H_
#define STORAGE_LEVELDB_DB_SNAPSHOT_H_

#include "db/dbformat.h"
#include "leveldb/db.h"

namespace leveldb {

class SnapshotList;

// Snapshots are kept in a doubly-linked list in the DB.
// Each SnapshotImpl corresponds to a particular sequence number.
/**
 * @brief 包含一个sequence number的双链表
 *
 */
class SnapshotImpl : public Snapshot {
  public:
    SnapshotImpl(SequenceNumber sequence_number)
        : sequence_number_(sequence_number) {}

    SequenceNumber sequence_number() const { return sequence_number_; }

  private:
    friend class
        SnapshotList;  //! 将SnapshotList声明为友元，可以访问Snapshot的私有成员以及私有析构函数，可以负责删除Snapshot对象

    // SnapshotImpl is kept in a doubly-linked circular list. The SnapshotList
    // implementation operates on the next/previous fields directly.
    SnapshotImpl* prev_;
    SnapshotImpl* next_;

    const SequenceNumber sequence_number_;

#if !defined(NDEBUG)
    SnapshotList* list_ = nullptr;
#endif  // !defined(NDEBUG)
};

class SnapshotList {
  public:
    SnapshotList() : head_(0) {
        head_.prev_ = &head_;
        head_.next_ = &head_;
    }

    bool empty() const { return head_.next_ == &head_; }

    /**
   * @brief 访问队首最老snapshot节点
   *
   * @return SnapshotImpl*
   */
    SnapshotImpl* oldest() const {
        assert(!empty());
        return head_.next_;
    }

    /**
   * @brief 访问队尾最新snapshot节点
   *
   * @return SnapshotImpl*
   */
    SnapshotImpl* newest() const {
        assert(!empty());
        return head_.prev_;
    }

    // Creates a SnapshotImpl and appends it to the end of the list.
    /**
   * @brief Snapshot的创建，并放入SnapshotList队尾，保证sequence number的有序性
   *
   * @param sequence_number
   * @return SnapshotImpl* 返回新创建的snapshot对象
   */
    SnapshotImpl* New(SequenceNumber sequence_number) {
        assert(empty() || newest()->sequence_number_ <= sequence_number);

        SnapshotImpl* snapshot = new SnapshotImpl(sequence_number);

#if !defined(NDEBUG)
        snapshot->list_ = this;
#endif  // !defined(NDEBUG)
        snapshot->next_ = &head_;
        snapshot->prev_ = head_.prev_;
        snapshot->prev_->next_ = snapshot;
        snapshot->next_->prev_ = snapshot;
        return snapshot;
    }

    // Removes a SnapshotImpl from this list.
    //
    // The snapshot must have been created by calling New() on this list.
    //
    // The snapshot pointer should not be const, because its memory is
    // deallocated. However, that would force us to change DB::ReleaseSnapshot(),
    // which is in the API, and currently takes a const Snapshot.
    /**
   * @brief 从列表中删除给定的snapshot，并负责析构Snapshot对象
   *
   * @param snapshot
   */
    void Delete(const SnapshotImpl* snapshot) {
#if !defined(NDEBUG)
        assert(snapshot->list_ == this);
#endif  // !defined(NDEBUG)
        snapshot->prev_->next_ = snapshot->next_;
        snapshot->next_->prev_ = snapshot->prev_;
        delete snapshot;
    }

  private:
    // Dummy head of doubly-linked list of snapshots
    SnapshotImpl head_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SNAPSHOT_H_
