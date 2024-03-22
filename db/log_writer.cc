// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cstdint>

#include "db/log_writer.h"
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

/**
 * @brief 为不同的record类型初始化一个crc32c码
 * @param type_crc
 */
static void InitTypeCrc(uint32_t *type_crc) {
    for (int i = 0; i <= kMaxRecordType; i++) {
        char t = static_cast<char>(i);
        type_crc[i] = crc32c::Value(&t, 1);
    }
}

Writer::Writer(WritableFile *dest) : dest_(dest), block_offset_(0) {
    InitTypeCrc(type_crc_);
}

Writer::Writer(WritableFile *dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
    InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;

/**
 * @brief
 * 将slice进行切分，作为多个record进行写入日志，并为每个record添加一个head
 * 其中record等价于文档中的chunk
 * @param slice
 * @return Status
 */
Status Writer::AddRecord(const Slice &slice) {
    const char *ptr = slice.data();
    size_t left = slice.size();  // data length

    // Fragment the record if necessary and emit it.  Note that if slice
    // is empty, we still want to iterate once to emit a single
    // zero-length record
    //! 如果slice过大，则进行分段，并发送record
    //! 如果slice为空，仍然发送一个空长度的record

    Status s;
    bool begin = true;
    //? 执行分段record并写入的操作
    do {
        const int leftover = kBlockSize - block_offset_;  // 当前block剩余字节数
        assert(leftover >= 0);

        //* 当剩余空间不够一个head的大小时，填0
        if (leftover < kHeaderSize) {
            // Switch to a new block
            if (leftover > 0) {
                // Fill the trailer (literal below relies on kHeaderSize being
                // 7)
                static_assert(kHeaderSize == 7, "");

                //* 写入剩余的leftover个'\0'字符，填充不用的空间
                dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
            }
            block_offset_ = 0;
        }

        // Invariant: we never leave < kHeaderSize bytes in a block.
        assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

        const size_t avail =
            kBlockSize - block_offset_ - kHeaderSize;  // block剩余可用空间
        const size_t fragment_length =
            (left < avail) ? left : avail;  // 当前block可以写入的data长度

        RecordType type;
        const bool end =
            (left ==
             fragment_length);  // 判断当前block是否能够写完这个slice的data

        if (begin && end) {
            type = kFullType;
        } else if (begin) {
            type = kFirstType;
        } else if (end) {
            type = kLastType;
        } else {
            type = kMiddleType;
        }

        //* 执行写入日志的操作
        s = EmitPhysicalRecord(type, ptr, fragment_length);

        // 更新分段的内容
        ptr += fragment_length;
        left -= fragment_length;
        begin = false;
    } while (s.ok() && left > 0);

    return s;
}

/**
 * @brief 执行一次日志的物理写入，即一条record/chunk
 *
 * @param t       当前record的类型 fulltype/firsttype/middletype/lasttype
 * @param ptr     当前执行写入data的开始地址
 * @param length  当前执行写入data的长度
 * @return Status
 */
Status Writer::EmitPhysicalRecord(RecordType t, const char *ptr,
                                  size_t length) {
    assert(length <= 0xffff);  // Must fit in two bytes //?
                               // 长度小于65535，length用两个字节表示
    assert(block_offset_ + kHeaderSize + length <=
           kBlockSize);  // 保证不超过一个block大小

    // Format the header
    //* 1.格式化head | checksum 4bytes | length 2bytes | type 1byte | data
    // length-bytes |
    //*             |             head                             |      data |
    char buf[kHeaderSize];

    // 取length的低16位，满足两字节需求，按照小端法存放
    buf[4] = static_cast<char>(length & 0xff);
    buf[5] = static_cast<char>(length >> 8);

    buf[6] = static_cast<char>(t);

    // Compute the crc of the record type and the payload.
    //* 2. 通过type和data计算crc32c校验码，填充到checksum位置
    uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
    crc = crc32c::Mask(crc);  // Adjust for storage
    // 调整字节顺序
    EncodeFixed32(buf, crc);

    // Write the header and the payload
    //* 3. 将head写入到日志文件中
    Status s = dest_->Append(Slice(buf, kHeaderSize));

    //* 4. 判断是否写入成功，成功则继续写入data到日志文件中
    if (s.ok()) {
        s = dest_->Append(Slice(ptr, length));
        if (s.ok()) {
            s = dest_->Flush();
        }
    }
    block_offset_ += kHeaderSize + length;
    return s;
}

}  // namespace log
}  // namespace leveldb
