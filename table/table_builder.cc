// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "leveldb/table_builder.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
    Rep(const Options& opt, WritableFile* f)
        : options(opt),
          index_block_options(opt),
          file(f),
          offset(0),
          data_block(&options),
          index_block(&index_block_options),
          num_entries(0),
          closed(false),
          filter_block(opt.filter_policy == nullptr
                           ? nullptr
                           : new FilterBlockBuilder(opt.filter_policy)),
          pending_index_entry(false) {
        index_block_options.block_restart_interval = 1;
    }

    Options options;
    Options index_block_options;
    WritableFile* file;
    uint64_t offset;  // 用来记录当前已经写入file的bytes大小
    Status status;
    BlockBuilder data_block;
    BlockBuilder index_block;
    std::string last_key;
    int64_t num_entries;
    bool closed;  // Either Finish() or Abandon() has been called.
    FilterBlockBuilder* filter_block;

    // We do not emit the index entry for a block until we have seen the
    // first key for the next data block.  This allows us to use shorter
    // keys in the index block.  For example, consider a block boundary
    // between the keys "the quick brown fox" and "the who".  We can use
    // "the r" as the key for the index block entry since it is >= all
    // entries in the first block and < all entries in subsequent
    // blocks.
    //* 在看见下一个data block的第一个key值之前，不生成一个index entry；这样做的好处是可以使用一个短key值来作为两个data block之间的边界
    //* 如第一个data block的最大key为 "the quick brown fox"，第二个data block的最小key为 "the who"，此时可以使用 "the r"作为index entry来保证使用短key作为两个data block之间的边界
    //
    // Invariant: r->pending_index_entry is true only if data_block is empty.
    bool pending_index_entry;    // 等待插入index entry的标志
    BlockHandle pending_handle;  // Handle to add to index block

    std::string compressed_output;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
    if (rep_->filter_block != nullptr) {
        rep_->filter_block->StartBlock(0);
    }
}

TableBuilder::~TableBuilder() {
    assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
    delete rep_->filter_block;
    delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
    // Note: if more fields are added to Options, update
    // this function to catch changes that should not be allowed to
    // change in the middle of building a Table.
    if (options.comparator != rep_->options.comparator) {
        return Status::InvalidArgument(
            "changing comparator while building table");
    }

    // Note that any live BlockBuilders point to rep_->options and therefore
    // will automatically pick up the updated options.
    rep_->options = options;
    rep_->index_block_options = options;
    rep_->index_block_options.block_restart_interval =
        1;  //* index_block通过对BlockBuilder的复用来实现，设置差分编码间隔为1，即不采用差分编码
    //* index entry的结构退化为key len | key | value len | value
    return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
    Rep* r = rep_;
    assert(!r->closed);
    if (!ok()) return;

    //* step1. 保证add是按照升序插入的
    if (r->num_entries > 0) {
        assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
    }

    //? step2. 在Flush的时候设置此标志位，等待下次data block开始时的第一条add key的时候再将index entry写入到index block中
    if (r->pending_index_entry) {
        assert(r->data_block.empty());
        // 用于寻找最短的key值，能够区分两个data block
        r->options.comparator->FindShortestSeparator(&r->last_key, key);
        std::string handle_encoding;
        r->pending_handle.EncodeTo(&handle_encoding); // 其中pending_handle在进行上次data block的WriteRawBlock时已经进行了设置
        // 添加一条index entry: max key | offset | length
        r->index_block.Add(r->last_key, Slice(handle_encoding));
        r->pending_index_entry = false;
    }

    //* step3. 将当前key记录添加到filter block中
    if (r->filter_block != nullptr) {
        r->filter_block->AddKey(key);
    }

    //* step4. 更新last key和当前data block的entry条数，添加data block
    r->last_key.assign(key.data(), key.size());
    r->num_entries++;
    r->data_block.Add(key, value);

    //* step5. 如果当前data block的数据大小已经超过了预设的block size，将其写入到磁盘中
    const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
    if (estimated_block_size >= r->options.block_size) {
        Flush();
    }
}

/**
 * @brief 将data block中的内容flush到文件中
 *
 */
void TableBuilder::Flush() {
    Rep* r = rep_;
    assert(!r->closed);
    if (!ok()) return;
    if (r->data_block.empty()) return;
    assert(!r->pending_index_entry);

    //* step1. 将data block的内容进行压缩并写入到文件中，同时设置对应的index entry的参数
    WriteBlock(&r->data_block, &r->pending_handle);
    if (ok()) {
        r->pending_index_entry = true;  // 设置更新index_entry的标志位，等待下次data block开始时进行写入
        r->status = r->file->Flush();
    }

    //* step2. 从r->offset再重新开始一个data block的滤波器，对应一个filter data
    if (r->filter_block != nullptr) {
        r->filter_block->StartBlock(r->offset);
    }
}

/**
 * @brief 写入一个blockbuilder的内容，进行压缩，同时添加trailer写入到文件中
 *
 * @param block
 * @param handle
 */
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
    // File format contains a sequence of blocks where each block has:
    //    block_data: uint8[n]
    //    type: uint8
    //    crc: uint32
    assert(ok());
    Rep* r = rep_;
    Slice raw = block->Finish();

    Slice block_contents;
    CompressionType type = r->options.compression;
    // TODO(postrelease): Support more compression options: zlib?
    switch (type) {
        case kNoCompression:
            block_contents = raw;
            break;

        case kSnappyCompression: {
            std::string* compressed = &r->compressed_output;
            if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
                compressed->size() < raw.size() - (raw.size() / 8u)) {
                block_contents = *compressed;
            } else {
                // Snappy not supported, or compressed less than 12.5%, so just
                // store uncompressed form
                block_contents = raw;
                type = kNoCompression;
            }
            break;
        }

        case kZstdCompression: {
            std::string* compressed = &r->compressed_output;
            if (port::Zstd_Compress(r->options.zstd_compression_level,
                                    raw.data(), raw.size(), compressed) &&
                compressed->size() < raw.size() - (raw.size() / 8u)) {
                block_contents = *compressed;
            } else {
                // Zstd not supported, or compressed less than 12.5%, so just
                // store uncompressed form
                block_contents = raw;
                type = kNoCompression;
            }
            break;
        }
    }
    WriteRawBlock(block_contents, type, handle);
    r->compressed_output.clear();
    block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
    Rep* r = rep_;
    handle->set_offset(r->offset);
    handle->set_size(block_contents.size());
    r->status = r->file->Append(block_contents);

    //* 物理结构，为每个block_size添加type + crc组成的trailer
    if (r->status.ok()) {
        char trailer[kBlockTrailerSize];
        trailer[0] = type;
        uint32_t crc =
            crc32c::Value(block_contents.data(), block_contents.size());
        crc =
            crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
        EncodeFixed32(trailer + 1, crc32c::Mask(crc));
        r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
        if (r->status.ok()) {
            r->offset += block_contents.size() + kBlockTrailerSize;
        }
    }
}

Status TableBuilder::status() const { return rep_->status; }

/**
 * @brief 完成整个sstable的写入
 * 对于只有一个block的管理数据，在最后进行写入
 *
 * @return Status
 */
Status TableBuilder::Finish() {
    Rep* r = rep_;
    Flush();        // 将剩余的data block内容flush到文件中
    assert(!r->closed);
    r->closed = true;

    BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

    //* 1. Write filter block
    //* filter block的类型为filter block builder，接口不一致无法使用WriteBlock
    //* 不进行压缩
    if (ok() && r->filter_block != nullptr) {
        WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                      &filter_block_handle);
    }

    //* 2. Write metaindex block
    if (ok()) {
        // 记录filter block的信息并写入
        BlockBuilder meta_index_block(&r->options);
        if (r->filter_block != nullptr) {
            // Add mapping from "filter.Name" to location of filter data
            std::string key = "filter.";
            key.append(r->options.filter_policy->Name());
            std::string handle_encoding;
            filter_block_handle.EncodeTo(&handle_encoding);
            meta_index_block.Add(key, handle_encoding);
        }

        // TODO(postrelease): Add stats and other meta blocks
        WriteBlock(&meta_index_block, &metaindex_block_handle);
    }

    //* 3. Write index block
    if (ok()) {
        if (r->pending_index_entry) {
            // 此时last_key是sstable中的最后一个key，找到这个key的最短后缀，并记录到最后一个index entry中
            r->options.comparator->FindShortSuccessor(&r->last_key);
            std::string handle_encoding;
            r->pending_handle.EncodeTo(&handle_encoding);
            r->index_block.Add(r->last_key, Slice(handle_encoding));
            r->pending_index_entry = false;
        }
        WriteBlock(&r->index_block, &index_block_handle);
    }

    //* 4. Write footer
    if (ok()) {
        Footer footer;
        footer.set_metaindex_handle(metaindex_block_handle);
        footer.set_index_handle(index_block_handle);
        std::string footer_encoding;
        footer.EncodeTo(&footer_encoding);
        // footer不用WriteBlock的形式，而是直接写入字符串，也没有trailer
        r->status = r->file->Append(footer_encoding);
        if (r->status.ok()) {
            r->offset += footer_encoding.size();
        }
    }
    return r->status;
}

void TableBuilder::Abandon() {
    Rep* r = rep_;
    assert(!r->closed);
    r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
