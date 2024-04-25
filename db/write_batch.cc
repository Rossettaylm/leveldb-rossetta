// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

//! rep_的数据存放格式
// WriteBatch::rep_ :=
//    sequence: fixed64     //* head = 8Bytes + 4Bytes
//    count: fixed32
//    data: record[count]   //* data
// record :=
//    kTypeValue varstring varstring     //* put:   type; keylen, keydata;  valuelen, valuedata
//    kTypeDeletion varstring            //* deletion: type; keylen, keydata   -- 删除操作没有数据
// varstring :=
//    len: varint32    //* 可变长32位数据，表示长度
//    data: uint8[len]

#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "util/coding.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;

WriteBatch::WriteBatch() { Clear(); }

WriteBatch::~WriteBatch() = default;

WriteBatch::Handler::~Handler() = default;

void WriteBatch::Clear() {
    rep_.clear();
    rep_.resize(kHeader);
}

size_t WriteBatch::ApproximateSize() const { return rep_.size(); }

/**
 * @brief 循环从rep中解析键值对，并通过Handler接口进行put和delete
 * @param handler 提供put和delete接口对memtable* 进行操作
 * @return Status
 */
Status WriteBatch::Iterate(Handler* handler) const {
    Slice input(rep_);
    if (input.size() < kHeader) {
        return Status::Corruption("malformed WriteBatch (too small)");
    }

    input.remove_prefix(kHeader);
    Slice key, value;
    int found = 0;

    while (!input.empty()) {
        found++;
        char type = input[0];
        input.remove_prefix(1);
        switch (type) {
            case kTypeValue:
                if (GetLengthPrefixedSlice(&input, &key) &&
                    GetLengthPrefixedSlice(&input, &value)) {
                    handler->Put(key, value);
                } else {
                    return Status::Corruption("bad WriteBatch Put");
                }
                break;
            case kTypeDeletion:
                if (GetLengthPrefixedSlice(&input, &key)) {
                    handler->Delete(key);
                } else {
                    return Status::Corruption("bad WriteBatch Delete");
                }
                break;
            default:
                return Status::Corruption("unknown WriteBatch tag");
        }
    }

    //* 判断一个batch中的record数量是否正确
    if (found != WriteBatchInternal::Count(this)) {
        return Status::Corruption("WriteBatch has wrong count");
    } else {
        return Status::OK();
    }
}

/**
 * @brief 计算batch中包含几条record记录
 * @param b
 * @return int
 */
int WriteBatchInternal::Count(const WriteBatch* b) {
    return DecodeFixed32(b->rep_.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
    EncodeFixed32(&b->rep_[8], n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
    return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
    EncodeFixed64(&b->rep_[0], seq);
}

/**
 * @brief 将put操作的键值对编码到WriteBatch中
 * @param key
 * @param value
 */
void WriteBatch::Put(const Slice& key, const Slice& value) {
    WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
    rep_.push_back(static_cast<char>(kTypeValue));
    //* 输入带有前置长度的slice，即keylen，key
    PutLengthPrefixedSlice(&rep_, key);
    PutLengthPrefixedSlice(&rep_, value);
}

/**
 * @brief 将Delete操作的键值对编码到WriteBatch中
 * @param key
 */
void WriteBatch::Delete(const Slice& key) {
    WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
    rep_.push_back(static_cast<char>(kTypeDeletion));
    PutLengthPrefixedSlice(&rep_, key);
}

void WriteBatch::Append(const WriteBatch& source) {
    WriteBatchInternal::Append(this, &source);
}

namespace {
class MemTableInserter : public WriteBatch::Handler {
  public:
    SequenceNumber sequence_;
    MemTable* mem_;

    void Put(const Slice& key, const Slice& value) override {
        mem_->Add(sequence_, kTypeValue, key, value);
        sequence_++;
    }
    void Delete(const Slice& key) override {
        mem_->Add(sequence_, kTypeDeletion, key, Slice());
        sequence_++;
    }
};
}  // namespace

/**
 * @brief 通过内部的迭代解析方法Iterate将WriteBatch中打包的内容rep插入到memtable中
 * @param b WriteBatch -- src
 * @param memtable MemTable -- dst
 * @return Status
 */
Status WriteBatchInternal::InsertInto(const WriteBatch* b, MemTable* memtable) {
    MemTableInserter inserter;
    inserter.sequence_ = WriteBatchInternal::Sequence(b);
    inserter.mem_ = memtable;
    return b->Iterate(&inserter);
}

/**
 * @brief 直接设置WriteBatch中的内容
 *
 * @param b
 * @param contents
 */
void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
    assert(contents.size() >= kHeader);
    b->rep_.assign(contents.data(), contents.size());
}

/**
 * @brief 将WriteBatch的内容进行合并，并去除头部
 * @param dst
 * @param src
 */
void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
    SetCount(dst, Count(dst) + Count(src));
    assert(src->rep_.size() >= kHeader);
    dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

}  // namespace leveldb
