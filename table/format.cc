// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/env.h"
#include "leveldb/options.h"
#include "port/port.h"
#include "table/block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

void BlockHandle::EncodeTo(std::string* dst) const {
    // Sanity check that all fields have been set
    assert(offset_ != ~static_cast<uint64_t>(0));
    assert(size_ != ~static_cast<uint64_t>(0));
    PutVarint64(dst, offset_);
    PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
    if (GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
        return Status::OK();
    } else {
        return Status::Corruption("bad block handle");
    }
}

/**
 * @brief 将Footer编码到字符串内
 * 通过padding保证footer的编码为固定大小
 *
 * @param dst
 */
void Footer::EncodeTo(std::string* dst) const {
    const size_t original_size = dst->size();
    metaindex_handle_.EncodeTo(dst);
    index_handle_.EncodeTo(dst);

    //? 为不满2 * 10大小的dst添加padding (不应该加上original_size吗?)
    dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding

    //* 添加tableMagicNumber作为校验码
    PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
    PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));

    assert(dst->size() == original_size + kEncodedLength);
    (void)original_size;  // Disable unused variable warning.
}

/**
 * @brief 将input中序列化字符串解码到footer内部
 *
 * @param input
 * @return Status
 */
Status Footer::DecodeFrom(Slice* input) {
    if (input->size() < kEncodedLength) {
        return Status::Corruption("not an sstable (footer too short)");
    }

    const char* magic_ptr = input->data() + kEncodedLength - 8;
    const uint32_t magic_lo = DecodeFixed32(magic_ptr);
    const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
    const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                            (static_cast<uint64_t>(magic_lo)));
    if (magic != kTableMagicNumber) {
        return Status::Corruption("not an sstable (bad magic number)");
    }

    Status result = metaindex_handle_.DecodeFrom(input);
    if (result.ok()) {
        result = index_handle_.DecodeFrom(input);
    }
    if (result.ok()) {
        // We skip over any leftover data (just padding for now) in "input"
        const char* end = magic_ptr + 8;
        *input = Slice(end, input->data() + input->size() - end);
    }
    return result;
}

/**
 * @brief 从file中读取一个block的数据
 *
 * @param file : RandomAccessFile* 随机存储文件
 * @param options : ReadOptions 读取选项
 * @param handle : const BlockHandle& 块句柄,指示Block所在位置及大小
 * @param result : BlockContents* 输出内容,用于存放输出信息
 * @return Status
 */
Status ReadBlock(RandomAccessFile* file, const ReadOptions& options,
                 const BlockHandle& handle, BlockContents* result) {
    result->data = Slice();
    result->cachable = false;
    result->heap_allocated = false;

    // Read the block contents as well as the type/crc footer.
    // See table_builder.cc for the code that built this structure.
    //* step1. 从堆上分配 block size + trailer size 大小的缓冲区用于存放block数据(scratch)
    size_t n = static_cast<size_t>(handle.size());
    char* buf = new char[n + kBlockTrailerSize];

    Slice contents;
    Status s =
        file->Read(handle.offset(), n + kBlockTrailerSize, &contents,
                   buf);  // 从file中读取block size + trailer size数据到contents中,同时通过buf作为scratch
    if (!s.ok()) {
        delete[] buf;  // 读取错误,释放内存
        return s;
    }
    if (contents.size() != n + kBlockTrailerSize) {
        delete[] buf;
        return Status::Corruption("truncated block read");
    }

    // Check the crc of the type and the block contents
    const char* data = contents.data();  // Pointer to where Read put the data
    //* step2. 如果设置了校验选项则进行校验
    if (options.verify_checksums) {
        const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
        const uint32_t actual = crc32c::Value(data, n + 1);
        if (actual != crc) {
            delete[] buf;
            s = Status::Corruption("block checksum mismatch");
            return s;
        }
    }

    //* step3. data[n]即trailer[0]: compression type,根据不同的压缩类型进行result赋值
    uint8_t compression_type = static_cast<uint8_t>(
        data[n]);  //? XXX(rossetta) 2024-04-16 15:27:33 新增,增加可读性
    switch (compression_type) {
        case kNoCompression:
            if (data != buf) {
                // File implementation gave us pointer to some other data.
                // Use it directly under the assumption that it will be live
                // while the file is open.
                //* File对象的实现给了我们实际数据contents(data)和scratch(buf)是不同的指针,这里我们直接使用data的,假设其内存在文件打开期间不会被释放
                // TODO(rossetta) 2024-04-16 15:33:37 是否意味着读取的数据被cache到了File内部?
                delete[] buf;
                result->data = Slice(data, n);
                result->heap_allocated = false;
                result->cachable = false;  // Do not double-cache //? 是否已经在File内部被cache过了?
            } else {
                //* 此时使用我们自己分配的堆内存存放了读取block的内容,需要手动释放
                result->data = Slice(buf, n);
                result->heap_allocated = true;
                result->cachable = true; // 设置为可以进行cache的
            }

            // Ok
            break;
        case kSnappyCompression: {
            size_t ulength = 0;
            if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
                delete[] buf;
                return Status::Corruption(
                    "corrupted snappy compressed block length");
            }
            char* ubuf = new char[ulength];
            if (!port::Snappy_Uncompress(data, n, ubuf)) {
                delete[] buf;
                delete[] ubuf;
                return Status::Corruption(
                    "corrupted snappy compressed block contents");
            }
            delete[] buf;
            result->data = Slice(ubuf, ulength);
            result->heap_allocated = true;
            result->cachable = true;
            break;
        }
        case kZstdCompression: {
            size_t ulength = 0;
            if (!port::Zstd_GetUncompressedLength(data, n, &ulength)) {
                delete[] buf;
                return Status::Corruption(
                    "corrupted zstd compressed block length");
            }
            char* ubuf = new char[ulength];
            if (!port::Zstd_Uncompress(data, n, ubuf)) {
                delete[] buf;
                delete[] ubuf;
                return Status::Corruption(
                    "corrupted zstd compressed block contents");
            }
            delete[] buf;
            result->data = Slice(ubuf, ulength);
            result->heap_allocated = true;
            result->cachable = true;
            break;
        }
        default:
            delete[] buf;
            return Status::Corruption("bad block type");
    }

    return Status::OK();
}

}  // namespace leveldb
