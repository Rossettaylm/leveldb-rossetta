// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cstdio>

#include "db/log_reader.h"
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Reader::Reporter::~Reporter() = default;

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0) {}

Reader::~Reader() { delete[] backing_store_; }

bool Reader::SkipToInitialBlock() {
    //* step1.计算Reader的block开始位置
    const size_t offset_in_block = initial_offset_ % kBlockSize;
    uint64_t block_start_location =
        initial_offset_ - offset_in_block;  // block开始的位置

    // Don't search a block if we'd be in the trailer
    //* 如果开始读取位置是在一个trailer中（即当前block中只有无用数据了）从下一个block开始
    if (offset_in_block > kBlockSize - 6) {
        block_start_location += kBlockSize;
    }

    //* step2.将buffer读取位置指定为block开始的位置，按block读取
    end_of_buffer_offset_ = block_start_location;

    // Skip to start of first block that can contain the initial record
    //* step3.将file_跳到block开始位置
    if (block_start_location > 0) {
        Status skip_status = file_->Skip(block_start_location);
        if (!skip_status.ok()) {
            ReportDrop(block_start_location, skip_status);
            return false;
        }
    }

    return true;
}

/**
 * @brief
 * 1. 将下一条记录读取到 *record 中。如果读取成功，返回 true；
 * 2. 如果到达输入的末尾，则返回 false。
 * 3. 可以使用 "*scratch" 作为临时存储空间。
 * 4. 填充到 *record 中的内容只有在该读取器上进行下一次修改操作或者对 *scratch
 * 进行下一次修改之前才会有效。
 *
 * @param record    数据输出 slice* 类型
 * @param scratch   临时数据存放 string* 类型，用于存放分段chunk并最终组成一条record
 * @return true 读取成功
 * @return false 读取到输入的末尾eof
 */
bool Reader::ReadRecord(Slice* record, std::string* scratch) {
    //* step1.用于指定文件偏移时，跳到Initial Block的位置，并设置相应的成员变量
    //* 一般是第一次读取时，用于跳到Initial Block位置
    if (last_record_offset_ < initial_offset_) {
        if (!SkipToInitialBlock()) {
            return false;
        }
    }

    //* 2. 执行这一步的read，先清除历史数据
    scratch->clear();
    record->clear();

    bool in_fragmented_record =
        false;  // 用于判断当前读取的record是否进行了分段操作

    // Record offset of the logical record that we're reading
    // 0 is a dummy value to make compilers happy
    //? 本次读取的record的开始偏移位置
    uint64_t prospective_record_offset = 0;

    Slice fragment;
    //! 循环读取，每次读取一条chunk/fragment，根据其type存入scratch中，最终组成一条record放入result中
    while (true) {
        // 读取一条chunk，并获取type
        const unsigned int record_type = ReadPhysicalRecord(&fragment);

        // ReadPhysicalRecord may have only had an empty trailer remaining in
        // its internal buffer. Calculate the offset of the next physical record
        // now that it has returned, properly accounting for its header size.
        //? 获取本次读取的chunk的物理开始位置
        //              | header | fragment |      buffer_      |
        //      physical_record_offset                    end_of_buffer_offset_
        uint64_t physical_record_offset = end_of_buffer_offset_ -
                                          buffer_.size() - kHeaderSize -
                                          fragment.size();

        //? 当initial_offset_大于0时，开启resyncing模式
        //? 此时，将跳过type类型为middle和last的chunk
        //? 用于重新定位到第一个first或者full类型，用于读取一个完整的record数据
        if (resyncing_) {
            if (record_type == kMiddleType) {
                continue;
            } else if (record_type == kLastType) {
                resyncing_ = false;
                continue;
            } else {
                resyncing_ = false;
            }
        }

        switch (record_type) {
            case kFullType:
                if (in_fragmented_record) {
                    // Handle bug in earlier versions of log::Writer where
                    // it could emit an empty kFirstType record at the tail end
                    // of a block followed by a kFullType or kFirstType record
                    // at the beginning of the next block.
                    if (!scratch->empty()) {
                        ReportCorruption(scratch->size(),
                                         "partial record without end(1)");
                    }
                }
                prospective_record_offset = physical_record_offset;
                scratch->clear();
                *record = fragment;
                last_record_offset_ = prospective_record_offset;
                return true;

            case kFirstType:
                if (in_fragmented_record) {
                    // Handle bug in earlier versions of log::Writer where
                    // it could emit an empty kFirstType record at the tail end
                    // of a block followed by a kFullType or kFirstType record
                    // at the beginning of the next block.
                    if (!scratch->empty()) {
                        ReportCorruption(scratch->size(),
                                         "partial record without end(2)");
                    }
                }
                prospective_record_offset = physical_record_offset;
                scratch->assign(fragment.data(), fragment.size());
                in_fragmented_record = true;
                break;

            case kMiddleType:
                if (!in_fragmented_record) {
                    ReportCorruption(fragment.size(),
                                     "missing start of fragmented record(1)");
                } else {
                    scratch->append(fragment.data(), fragment.size());
                }
                break;

            case kLastType:
                if (!in_fragmented_record) {
                    ReportCorruption(fragment.size(),
                                     "missing start of fragmented record(2)");
                } else {
                    scratch->append(fragment.data(), fragment.size());
                    *record = Slice(*scratch);
                    last_record_offset_ = prospective_record_offset;
                    return true;
                }
                break;

            case kEof:
                if (in_fragmented_record) {
                    // This can be caused by the writer dying immediately after
                    // writing a physical record but before completing the next;
                    // don't treat it as a corruption, just ignore the entire
                    // logical record.
                    scratch->clear();
                }
                return false;

            case kBadRecord:
                if (in_fragmented_record) {
                    ReportCorruption(scratch->size(),
                                     "error in middle of record");
                    in_fragmented_record = false;
                    scratch->clear();
                }
                break;

            default: {
                char buf[40];
                std::snprintf(buf, sizeof(buf), "unknown record type %u",
                              record_type);
                ReportCorruption((fragment.size() +
                                  (in_fragmented_record ? scratch->size() : 0)),
                                 buf);
                in_fragmented_record = false;
                scratch->clear();
                break;
            }
        }
    }
    return false;
}

uint64_t Reader::LastRecordOffset() { return last_record_offset_; }

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
    ReportDrop(bytes, Status::Corruption(reason));
}

/**
 * @brief 报告丢弃的字节数据
 *
 * @param bytes 丢弃数据大小
 * @param reason
 */
void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
    //* reporter_非空
    //* 丢弃的bytes数据总是buffer_的前缀，需要保证开始丢弃的位置不超过初始位置
    if (reporter_ != nullptr &&
        end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
        reporter_->Corruption(static_cast<size_t>(bytes), reason);
    }
}

/**
 * @brief 读取一条chunk(而非是record)
 * //! 需要注意的是，为保证读取的访问效率，Reader会一次性从文件系统中读取一个block的数据放入buffer_中
 * @param result
 * @return unsigned int
 */
unsigned int Reader::ReadPhysicalRecord(Slice* result) {
    while (true) {
        //? buffer_用于每次从文件系统中获取一个block的数据并放入内存中
        //* step1.先判断buffer_中是否还有缓存数据，如果没有（小于headsize）即从file_中读取一个block
        if (buffer_.size() < kHeaderSize) {
            // 此时需要从file_中读取一个block的数据
            if (!eof_) {
                // Last read was a full read, so this is a trailer to skip
                //* trailer指实际所需要的data的尾部信息，进行跳过
                buffer_.clear();
                //* 从file_中读取一个block的所有信息并放入buffer中
                Status status =
                    file_->Read(kBlockSize, &buffer_, backing_store_);
                end_of_buffer_offset_ += buffer_.size();
                if (!status.ok()) {
                    buffer_.clear();
                    ReportDrop(kBlockSize, status);
                    eof_ = true;
                    return kEof;
                } else if (buffer_.size() < kBlockSize) {
                    eof_ = true;
                }
                continue;
            } else {
                // Note that if buffer_ is non-empty, we have a truncated header
                // at the end of the file, which can be caused by the writer
                // crashing in the middle of writing the header. Instead of
                // considering this an error, just report EOF.
                //* 如果buffer_不为空，我们在文件末尾有一个不完整的头部（header），这可能是由于写入者在写头部的过程中崩溃导致的。我们不会将此视为一个错误，而是简单地报告文件结束（EOF）。
                buffer_.clear();
                return kEof;
            }
        }

        // Parse the header
        //* step2.从buffer_中读取一个chunk头部，进行检查
        const char* header = buffer_.data();
        // 详见Writer中head的格式 checksum4 + length2 + type1
        const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
        const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
        const unsigned int type = header[6];
        const uint32_t length = a | (b << 8);

        //? 判断如果buffer_中的残余数据不够一条chunk，则将数据丢弃
        //? chunk是Writer进行日志写入的最小单位，此时需要报告badrecord错误
        if (kHeaderSize + length > buffer_.size()) {
            size_t drop_size = buffer_.size();
            buffer_.clear();
            if (!eof_) {
                ReportCorruption(drop_size, "bad record length");
                return kBadRecord;
            }
            // If the end of the file has been reached without reading |length|
            // bytes of payload, assume the writer died in the middle of writing
            // the record. Don't report a corruption.
            //* 如果文件已经到达eof但是剩余数据不满足一条chunk时，假设Writer在写入到一半时崩溃了，此时仅仅返回eof
            return kEof;
        }

        //* step3.跳过zerotype类型且长度为0的chunk，该类型由mmap（env_posix.cc）进行内存预分配时产生
        if (type == kZeroType && length == 0) {
            // Skip zero length record without reporting any drops since
            // such records are produced by the mmap based writing code in
            // env_posix.cc that preallocates file regions.
            buffer_.clear();
            return kBadRecord;
        }

        // Check crc
        //* step4.如果开启了crc校验，则进行检查
        if (checksum_) {
            uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
            uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
            if (actual_crc != expected_crc) {
                // Drop the rest of the buffer since "length" itself may have
                // been corrupted and if we trust it, we could find some
                // fragment of a real log record that just happens to look
                // like a valid log record.
                size_t drop_size = buffer_.size();
                buffer_.clear();
                ReportCorruption(drop_size, "checksum mismatch");
                return kBadRecord;
            }
        }

        //* step5.删除掉已经读取的数据
        buffer_.remove_prefix(kHeaderSize + length);

        // Skip physical record that started before initial_offset_
        //* step6.判断读取的chunk开始位置是否在初始位置之前，如果是，则舍弃
        if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length <
            initial_offset_) {
            result->clear();
            return kBadRecord;
        }

        //* step7.返回数据部分
        *result = Slice(header + kHeaderSize, length);
        return type;
    }
}

}  // namespace log
}  // namespace leveldb
