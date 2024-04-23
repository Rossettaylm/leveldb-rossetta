// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/filename.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

struct TableAndFile {
    RandomAccessFile* file;
    Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {
    TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
    delete tf->table;
    delete tf->file;
    delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
    Cache* cache = reinterpret_cast<Cache*>(arg1);
    Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
    cache->Release(h);
}

TableCache::TableCache(const std::string& dbname, const Options& options,
                       int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {}

TableCache::~TableCache() { delete cache_; }

/**
 * @brief 从cache中查找table，如果不存在则从文件中读取并插入到cache中
 *
 * @param file_number
 * @param file_size
 * @param handle
 * @return Status
 */
Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
    Status s;
    char buf[sizeof(file_number)];
    EncodeFixed64(buf, file_number);
    Slice key(buf, sizeof(buf));
    *handle = cache_->Lookup(key);
    if (*handle == nullptr) {
        //* cache中未找到table对象，从文件中打开sstable
        std::string fname = TableFileName(dbname_, file_number);
        RandomAccessFile* file = nullptr;
        Table* table = nullptr;
        s = env_->NewRandomAccessFile(fname, &file);
        if (!s.ok()) {
            std::string old_fname = SSTTableFileName(dbname_, file_number);
            if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
                s = Status::OK();
            }
        }
        //* 从file中打开sstable对象
        if (s.ok()) {
            s = Table::Open(options_, file, file_size, &table);
        }

        if (!s.ok()) {
            assert(table == nullptr);
            delete file;
            // We do not cache error results so that if the error is transient,
            // or somebody repairs the file, we recover automatically.
        } else {
            //* 构建TableAndFile对象，用于存储sstable对应的file*和Table*
            TableAndFile* tf = new TableAndFile;
            tf->file = file;
            tf->table = table;
            //* 加入cache中
            *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
        }
    }
    return s;
}

/**
 * @brief 生成一个指向sstable的迭代器
 *
 * @param options 读取选项
 * @param file_number sstable文件号
 * @param file_size sstable文件大小
 * @param tableptr 输出指针，指向table对象
 * @return Iterator*
 */
Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table** tableptr) {
    //* step1. 将output赋予空值
    if (tableptr != nullptr) {
        *tableptr = nullptr;
    }

    //* step2. 从tablecache中查找table对象
    Cache::Handle* handle = nullptr;
    Status s = FindTable(file_number, file_size, &handle);
    if (!s.ok()) {
        return NewErrorIterator(s);
    }

    //* step3. table获取成功，则生成一个二级迭代器并注册回调函数，并返回
    Table* table =
        reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    Iterator* result = table->NewIterator(options);
    result->RegisterCleanup(&UnrefEntry, cache_, handle);

    //* step4. 将找到的table作为output传出去
    if (tableptr != nullptr) {
        *tableptr = table;
    }
    return result;
}

/**
 * @brief 获取table对象并查询其内的key值是否存在，如果存在则通过handle_result函数接口进行处理
 *
 * @param options
 * @param file_number
 * @param file_size
 * @param k
 * @param arg
 * @param handle_result
 * @return Status
 */
Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)) {
    Cache::Handle* handle = nullptr;
    Status s = FindTable(file_number, file_size, &handle);
    if (s.ok()) {
        Table* t =
            reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
        s = t->InternalGet(options, k, arg, handle_result);
        cache_->Release(handle);
    }
    return s;
}

void TableCache::Evict(uint64_t file_number) {
    //* step1. 从file number编码对应的entry key值
    char buf[sizeof(file_number)];
    EncodeFixed64(buf, file_number);

    //* step2. 将entry从cache中删除
    cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
