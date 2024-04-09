// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbformat.h"
#include "db/memtable.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

/**
 * @brief 从data开始的位置获取附带len的key或者value值
 *
 * @param data
 * @return Slice
 */
static Slice GetLengthPrefixedSlice(const char* data) {
    uint32_t len;
    const char* p = data;
    p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
    return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator& internal_key_comparator)
    : comparator_(internal_key_comparator),
      refs_(0),
      table_(comparator_, &arena_) {}

MemTable::~MemTable() { assert(refs_ == 0); }

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

/**
 * @brief 进行key的比较 -- 跳表中使用的比较函数
 *
 * @param aptr 从WriteBatch中封装的keyLen, key字符串
 * @param bptr 从WriteBatch中封装的KeyLen，key字符串
 * @return int
 */
int MemTable::KeyComparator::operator()(const char* aptr,
                                        const char* bptr) const {
    // Internal keys are encoded as length-prefixed strings.
    //* step1. 从batch的record数据中解析出InternalKey
    Slice a = GetLengthPrefixedSlice(aptr);
    Slice b = GetLengthPrefixedSlice(bptr);

    //* step2. 调用内部的InternalKeyComparator成员进行compare
    return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
/**
 * @brief 将target进行编码，得到keylen + key的形式，并存入scratch中
 *
 * @param scratch 编码输出结果 : std::string*
 * @param target  需要编码的key : const Slice&
 * @return const char*
 */
static const char* EncodeKey(std::string* scratch, const Slice& target) {
    scratch->clear();
    PutVarint32(scratch, target.size());
    scratch->append(target.data(), target.size());
    return scratch->data();
}

/**
 * @brief 对跳表迭代器的一层封装
 */
class MemTableIterator : public Iterator {
  public:
    explicit MemTableIterator(MemTable::Table* table) : iter_(table) {}

    MemTableIterator(const MemTableIterator&) = delete;
    MemTableIterator& operator=(const MemTableIterator&) = delete;

    ~MemTableIterator() override = default;

    bool Valid() const override { return iter_.Valid(); }
    void Seek(const Slice& k) override {
        iter_.Seek(EncodeKey(&tmp_, k));
    }  // TODO(rossetta) 2024-04-08 11:12:30 k是什么？有无带tag？
    void SeekToFirst() override { iter_.SeekToFirst(); }
    void SeekToLast() override { iter_.SeekToLast(); }
    void Next() override { iter_.Next(); }
    void Prev() override { iter_.Prev(); }
    Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
    Slice value() const override {
        Slice key_slice = key();
        Slice value_slice = GetLengthPrefixedSlice(
            key_slice.data() +
            key_slice
                .size());  // key_slice.data() + key_slice.size() = value_slice.data()
        return value_slice;
    }

    Status status() const override { return Status::OK(); }

  private:
    MemTable::Table::Iterator iter_;
    std::string tmp_;  // For passing to EncodeKey
};

Iterator* MemTable::NewIterator() { return new MemTableIterator(&table_); }

/**
 * @brief 将key和value编码成InternalKey.size + InternalKey + Value.size() + Value = entry的形式，并分配空间存入跳表中
 * 其中InternalKey包含userkey + sequence + type
 * @param s 当前操作的序列号
 * @param type 操作类型 put/delete
 * @param key 原始key
 * @param value 原始value
 */
void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key,
                   const Slice& value) {
    //* 跳表中存放的key的编码内容如下
    // Format of an entry is concatenation of:
    //* InternalKey.size() + InternalKey
    //  key_size     : varint32 of internal_key.size()
    //  key bytes    : char[internal_key.size()]
    //  tag          : uint64((sequence << 8) | type)
    //* value.size() + value
    //  value_size   : varint32 of value.size()
    //  value bytes  : char[value.size()]

    size_t key_size = key.size();
    size_t val_size = value.size();

    size_t internal_key_size =
        key_size + 8;  //* 计算InternalKey的大小，即uKey + 8

    //* step1. 通过变长编码计算总体keylen + key + valuelen + value的总长度
    const size_t encoded_len = VarintLength(internal_key_size) +
                               internal_key_size + VarintLength(val_size) +
                               val_size;

    //* step2. 内存分配
    char* buf = arena_.Allocate(encoded_len);

    //* step3. 编码internalkey并进行存放
    char* p = EncodeVarint32(buf, internal_key_size);
    std::memcpy(p, key.data(), key_size);
    p += key_size;

    EncodeFixed64(
        p, (s << 8) | type);  // 用高位7个字节存放sequence，低位1个字节存放type
    p += 8;

    //* step5. 编码value并进行存放
    p = EncodeVarint32(p, val_size);
    std::memcpy(p, value.data(), val_size);
    assert(p + val_size == buf + encoded_len);

    //* step6. 将table_key的内容插入到表中
    table_.Insert(buf);
}

/**
 * @brief 从跳表中查询一条key记录
 *
 * @param key 需要查询的key: LoopupKey
 * @param value 输出的value : std::string*
 * @param s 返回查询状态 : Status*
 * @return true
 * @return false
 */
bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
    Slice memkey =
        key.memtable_key();  // memtable_key 包含 keylen, internal_key，用于跳表内部存放
    Table::Iterator iter(&table_);
    iter.Seek(memkey.data());  // 定位查找到key的位置

    //* 查询到了，进行解析
    //* iter的情况:
    //* 1. iter->key == memkey
    //* 2. iter->key > memkey   // 插入位置
    //* 3. iter->key == nullptr // 没有找到大于等于memkey的元素
    if (iter.Valid()) {
        // entry format is:
        //    klength  varint32
        //    userkey  char[klength]
        //    tag      uint64
        //    vlength  varint32
        //    value    char[vlength]
        // Check that it belongs to same user key.  We do not check the
        // sequence number since the Seek() call above should have skipped
        // all entries with overly large sequence numbers.
        const char* entry = iter.key();
        uint32_t key_length;
        const char* key_ptr = GetVarint32Ptr(
            entry, entry + 5, &key_length);  // key_ptr指向key的开始位置

        //* 通过user_key进行比较，判断是否是所查找的key
        if (comparator_.comparator.user_comparator()->Compare(
                Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
            // Correct user key
            const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
            switch (static_cast<ValueType>(tag & 0xff)) {
                case kTypeValue: {
                    Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
                    value->assign(v.data(), v.size());
                    return true;
                }
                case kTypeDeletion:
                    *s = Status::NotFound(Slice());
                    return true;
            }
        }
    }
    return false;
}

}  // namespace leveldb
