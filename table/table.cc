// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "leveldb/table.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

struct Table::Rep {
    ~Rep() {
        delete filter;
        delete[] filter_data;
        delete index_block;
    }

    Options options;
    Status status;
    RandomAccessFile* file;  // 存储sstable文件的file
    uint64_t cache_id;
    FilterBlockReader* filter;  // filter block读取器
    const char* filter_data;    // 读取得到的filter data指针

    BlockHandle
        metaindex_handle;  // Handle to metaindex_block: saved from footer  -- footer中保存的mate index block handle，用于定位filter block
    Block*
        index_block;  // index block对象，通过内部实现的Block::Iter进行迭代访问其entry
};

/**
 * @brief static函数: 打开一个sstable并返回打开状态
 *
 * @param options db设置选项
 * @param file 随机读取文件,用于持久化存储sstable
 * @param size sstable文件的大小
 * @param table : Table ** 用于输出打开的table
 * @return Status
 */
Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table) {
    *table = nullptr;
    if (size < Footer::kEncodedLength) {
        return Status::Corruption("file is too short to be an sstable");
    }

    //* step1. 解析footer
    char footer_space[Footer::kEncodedLength];
    Slice footer_input;
    //* 读取file的尾部Footer::kEncodeLength长度字节,获取footer的序列化信息
    Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                          &footer_input, footer_space);
    if (!s.ok()) return s;

    Footer footer;
    s = footer.DecodeFrom(&footer_input);
    if (!s.ok()) return s;

    // Read the index block
    //* step2. 读取index block元数据,获取索引信息以快速查询
    BlockContents index_block_contents;
    ReadOptions opt;
    if (options.paranoid_checks) {  // 设置严格读取校验
        opt.verify_checksums = true;
    }
    s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

    //* step3. 读取footer和index block成功,开始构建Table对象并从file中读取元数据到rep中
    if (s.ok()) {
        // We've successfully read the footer and the index block: we're
        // ready to serve requests.
        Block* index_block = new Block(index_block_contents);
        Rep* rep = new Table::Rep;
        rep->options = options;
        rep->file = file;
        rep->metaindex_handle = footer.metaindex_handle();
        rep->index_block = index_block;
        rep->cache_id =
            (options.block_cache ? options.block_cache->NewId() : 0);
        rep->filter_data = nullptr;
        rep->filter = nullptr;
        *table = new Table(rep);
        (*table)->ReadMeta(footer);
    }

    return s;
}

void Table::ReadMeta(const Footer& footer) {
    //* step1. 如果options中没有指定滤波器，则不需要元数据读取
    if (rep_->options.filter_policy == nullptr) {
        return;  // Do not need any metadata
    }

    // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
    // it is an empty block.
    //* step2. 指定读取选项
    ReadOptions opt;
    if (rep_->options.paranoid_checks) {
        opt.verify_checksums = true;
    }

    //* step3. 通过ReadBlock读取file中的meta index block，通过其间接访问filter block
    BlockContents contents;
    if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents)
             .ok()) {
        // Do not propagate errors since meta info is not needed for operation
        return;
    }

    //* step4. 生成meta index block对象，通过迭代器访问其唯一的一条记录，即key = "filter." + filter.Name(); value = offset + length
    Block* meta = new Block(contents);

    Iterator* iter = meta->NewIterator(BytewiseComparator());
    std::string key = "filter.";
    key.append(rep_->options.filter_policy->Name());
    iter->Seek(key);

    //* step5. meta index block唯一键值对为有效值，此时根据其value读取filter block的信息
    if (iter->Valid() && iter->key() == Slice(key)) {
        ReadFilter(iter->value());
    }

    //* step6. 释放分配的meta index block和block iter
    delete iter;
    delete meta;
}

/**
 * @brief 从filter_handle的序列化字符串中读取file文件中的filter block
 *
 * @param filter_handle_value : const Slice& 从meta index block解析得到的filter block handle序列化信息
 */
void Table::ReadFilter(const Slice& filter_handle_value) {
    //* step1. 进行filter handle的解码
    Slice v = filter_handle_value;
    BlockHandle filter_handle;
    if (!filter_handle.DecodeFrom(&v).ok()) {
        return;
    }

    // We might want to unify with ReadBlock() if we start
    // requiring checksum verification in Table::Open.
    //* step2. 从file文件中读取filter block
    ReadOptions opt;
    if (rep_->options.paranoid_checks) {
        opt.verify_checksums = true;
    }
    BlockContents filter_block;
    if (!ReadBlock(rep_->file, opt, filter_handle, &filter_block).ok()) {
        return;
    }
    if (filter_block.heap_allocated) {
        rep_->filter_data =
            filter_block.data.data();  // Will need to delete later
    }
    rep_->filter =
        new FilterBlockReader(rep_->options.filter_policy, filter_block.data);
}

Table::~Table() { delete rep_; }

static void DeleteBlock(void* arg, void* ignored) {
    delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
    Block* block = reinterpret_cast<Block*>(value);
    delete block;
}

static void ReleaseBlock(void* arg, void* h) {
    Cache* cache = reinterpret_cast<Cache*>(arg);
    Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
    cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
/**
 * @brief 通过blockhandle或者index entry value来读取data block的内容，并返回一个可以遍历这个block的迭代器
 *
 * @param arg : void* Table*对象
 * @param options : const ReadOptions& 读取选项
 * @param index_value : const Slice& 索引值，代表了blockhandle的编码值，或者index block entry的value也是由offset + length组成的
 * @return Iterator* block迭代器，可以遍历block内部的entry
 */
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
    Table* table = reinterpret_cast<Table*>(arg);
    Cache* block_cache = table->rep_->options.block_cache;
    Block* block = nullptr;
    Cache::Handle* cache_handle = nullptr;

    BlockHandle handle;
    Slice input = index_value;
    Status s = handle.DecodeFrom(&input);
    // We intentionally allow extra stuff in index_value so that we
    // can add more features in the future.

    if (s.ok()) {
        BlockContents contents;
        if (block_cache != nullptr) {
            char cache_key_buffer[16];
            EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
            EncodeFixed64(cache_key_buffer + 8, handle.offset());
            Slice key(cache_key_buffer, sizeof(cache_key_buffer));
            cache_handle = block_cache->Lookup(key);
            if (cache_handle != nullptr) {
                block =
                    reinterpret_cast<Block*>(block_cache->Value(cache_handle));
            } else {
                s = ReadBlock(table->rep_->file, options, handle, &contents);
                if (s.ok()) {
                    block = new Block(contents);
                    if (contents.cachable && options.fill_cache) {
                        cache_handle = block_cache->Insert(
                            key, block, block->size(), &DeleteCachedBlock);
                    }
                }
            }
        } else {
            // 没有使用cache，从file中读取一个block
            s = ReadBlock(table->rep_->file, options, handle, &contents);
            if (s.ok()) {
                block = new Block(contents);
            }
        }
    }

    Iterator* iter;
    if (block != nullptr) {
        iter = block->NewIterator(table->rep_->options.comparator);
        if (cache_handle == nullptr) {
            // 在iter析构时，同时执行DeleteBlock函数
            iter->RegisterCleanup(&DeleteBlock, block, nullptr);
        } else {
            iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
        }
    } else {
        iter = NewErrorIterator(s);
    }
    return iter;
}

Iterator* Table::NewIterator(const ReadOptions& options) const {
    return NewTwoLevelIterator(
        rep_->index_block->NewIterator(rep_->options.comparator),
        &Table::BlockReader, const_cast<Table*>(this), options);
}

/**
 * @brief 即在sstable中查找一个key，并对其进行处理
 * 1. 一级查找：根据index block的迭代器进行粗略定位，获取data block
 * 2. 二级查找：根据data block的迭代器进行详细定位和seek
 *
 * @param options : const ReadOptions& 读取选项
 * @param k : const Slice& 需要读取的key
 * @param arg : void * handle_result函数的第一个参数
 * @param handle_result 函数指针，用于对查询到的key-value对进行处理
 * @return Status
 */
Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
    //* step1. 获取index block iteator，并定位key的位置（通过index block进行一级查找）
    Status s;
    Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
    iiter->Seek(k);  // 寻找一个大于等于k的index entry

    // iiter->Valid()表示查找的index block entry的max key(data block的key右边界)存在且大于等于k
    if (iiter->Valid()) {
        Slice handle_value = iiter->value();
        FilterBlockReader* filter = rep_->filter;
        BlockHandle handle;
        //* step2* 通过filter block进行过滤
        if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
            !filter->KeyMayMatch(handle.offset(), k)) {
            // 未通过过滤器，则未找到
            // Not found
        } else {
            //* step3. 通过了过滤器，定位对应的data block（进行二级查找）
            // iiter->value()对应了一个data block，且k小于等于这个data block的最大值，即k位于这个data block内部
            // 读取了对应的data block，并返回一个data block iter用于访问内部键值对
            Iterator* block_iter = BlockReader(this, options, iiter->value());
            block_iter->Seek(k);  // 二级查找

            //* step4. 查找到了条项，并进行handle_result处理
            if (block_iter->Valid()) {
                // 查找到了，进行处理
                (*handle_result)(arg, block_iter->key(), block_iter->value());
            }
            s = block_iter->status();
            delete block_iter;  // iter的析构函数执行了对block的释放或者删除
        }
    }
    if (s.ok()) {
        s = iiter->status();
    }
    delete iiter;
    return s;
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
    Iterator* index_iter =
        rep_->index_block->NewIterator(rep_->options.comparator);
    index_iter->Seek(key);
    uint64_t result;
    if (index_iter->Valid()) {
        BlockHandle handle;
        Slice input = index_iter->value();
        Status s = handle.DecodeFrom(&input);
        if (s.ok()) {
            result = handle.offset();
        } else {
            // Strange: we can't decode the block handle in the index block.
            // We'll just return the offset of the metaindex block, which is
            // close to the whole file size for this case.
            result = rep_->metaindex_handle.offset();
        }
    } else {
        // key is past the last key in the file.  Approximate the offset
        // by returning the offset of the metaindex block (which is
        // right near the end of the file).
        result = rep_->metaindex_handle.offset();
    }
    delete index_iter;
    return result;
}

}  // namespace leveldb
