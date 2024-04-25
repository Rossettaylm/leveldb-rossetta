// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {

Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
    Status s;
    meta->file_size = 0;
    //* step1. 定位到iter的开头，从头开始写入数据
    iter->SeekToFirst();

    std::string fname = TableFileName(dbname, meta->number);
    //* step2. 如果存在数据，则新建文件，通过TableBuilder开始写入table file
    if (iter->Valid()) {
        WritableFile* file;
        s = env->NewWritableFile(fname, &file);
        if (!s.ok()) {
            return s;
        }

        TableBuilder* builder = new TableBuilder(options, file);
        meta->smallest.DecodeFrom(
            iter->key());  // 将memtable中最小的key解码到meta data中
        Slice key;
        // 遍历memtable，并添加键值对到sstable中
        for (; iter->Valid(); iter->Next()) {
            key = iter->key();
            builder->Add(key, iter->value());
        }
        // 将memtable中的最大key记录到meta data中
        if (!key.empty()) {
            meta->largest.DecodeFrom(key);
        }

        // Finish and check for builder errors
        s = builder->Finish();
        if (s.ok()) {
            meta->file_size = builder->FileSize();  // 记录sstable大小
            assert(meta->file_size > 0);
        }
        delete builder;

        // Finish and check for file errors
        //* step3. 将sstable 对象持久化到磁盘文件中
        if (s.ok()) {
            s = file->Sync();
        }
        if (s.ok()) {
            s = file->Close();
        }
        delete file;
        file = nullptr;

        if (s.ok()) {
            // Verify that the table is usable
            //* step4. 通过打开其TableCache视图来证实写入的table file是可用的
            Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                                    meta->file_size);
            s = it->status();
            delete it;  // 删除
        }
    }

    // Check for input iterator errors
    if (!iter->status().ok()) {
        s = iter->status();
    }

    if (s.ok() && meta->file_size > 0) {
        // Keep it
    } else {
        env->RemoveFile(fname);  // 如果写入错误，则删除磁盘中的文件
    }
    return s;
}

}  // namespace leveldb
