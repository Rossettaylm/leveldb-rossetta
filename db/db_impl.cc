// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <set>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/db_impl.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
/**
 * @brief writer对象负责一个线程的一次write操作
 *
 */
struct DBImpl::Writer {
    explicit Writer(port::Mutex* mu)
        : batch(nullptr), sync(false), done(false), cv(mu) {}

    Status status;
    WriteBatch* batch;
    bool sync;
    bool done;
    port::CondVar cv;  // 用于通知或等待其他线程的cv
};

struct DBImpl::CompactionState {
    // Files produced by compaction
    struct Output {
        uint64_t number;
        uint64_t file_size;
        InternalKey smallest, largest;
    };

    Output* current_output() { return &outputs[outputs.size() - 1]; }

    explicit CompactionState(Compaction* c)
        : compaction(c),
          smallest_snapshot(0),
          outfile(nullptr),
          builder(nullptr),
          total_bytes(0) {}

    Compaction* const compaction;

    // Sequence numbers < smallest_snapshot are not significant since we
    // will never have to service a snapshot below smallest_snapshot.
    // Therefore if we have seen a sequence number S <= smallest_snapshot,
    // we can drop all entries for the same key with sequence numbers < S.
    SequenceNumber smallest_snapshot;

    std::vector<Output> outputs;

    // State kept for output being generated
    WritableFile* outfile;
    TableBuilder* builder;

    uint64_t total_bytes;
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
    if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
    if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

/**
 * @brief 净化Options，使其符合标准，保证用户设定的Options不超出限制
 *
 * @param dbname
 * @param icmp
 * @param ipolicy
 * @param src
 * @return Options
 */
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
    Options result = src;
    result.comparator = icmp;  // 设置comparator
    result.filter_policy = (src.filter_policy != nullptr)
                               ? ipolicy
                               : nullptr;  // 设置filter_policy

    // 设置相应的size范围
    ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
    ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
    ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
    ClipToRange(&result.block_size, 1 << 10, 4 << 20);

    // 创建info logger文件
    if (result.info_log == nullptr) {
        // Open a log file in the same directory as the db
        src.env->CreateDir(dbname);  // In case it does not exist
        src.env->RenameFile(InfoLogFileName(dbname),
                            OldInfoLogFileName(dbname));
        Status s =
            src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
        if (!s.ok()) {
            // No place suitable for logging
            result.info_log = nullptr;
        }
    }

    // 创建block cache
    if (result.block_cache == nullptr) {
        result.block_cache = NewLRUCache(8 << 20);  // 8MB
    }

    return result;
}

static int TableCacheSize(const Options& sanitized_options) {
    // Reserve ten files or so for other uses and give the rest to TableCache.
    return sanitized_options.max_open_files -
           kNumNonTableCacheFiles;  // 留下10个文件作为他用，其余的放入tablecache中，即tablecache.size()
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      shutting_down_(false),
      background_work_finished_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr),
      has_imm_(false),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      seed_(0),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      manual_compaction_(nullptr),
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_)) {}

DBImpl::~DBImpl() {
    // Wait for background work to finish.
    mutex_.Lock();
    //* step1. 设置db关闭标志
    shutting_down_.store(
        true, std::memory_order_release);  // 保证写操作对后续的指令是可见的
    //* step2. 等待后台任务完成
    while (background_compaction_scheduled_) {
        background_work_finished_signal_.Wait();
    }
    mutex_.Unlock();

    if (db_lock_ != nullptr) {
        env_->UnlockFile(db_lock_);
    }

    delete versions_;
    if (mem_ != nullptr) mem_->Unref();
    if (imm_ != nullptr) imm_->Unref();
    delete tmp_batch_;
    delete log_;
    delete logfile_;
    delete table_cache_;

    if (owns_info_log_) {
        delete options_.info_log;
    }
    if (owns_cache_) {
        delete options_.block_cache;
    }
}

Status DBImpl::NewDB() {
    VersionEdit new_db;
    new_db.SetComparatorName(user_comparator()->Name());
    new_db.SetLogNumber(0);
    new_db.SetNextFile(2);
    new_db.SetLastSequence(0);

    const std::string manifest = DescriptorFileName(dbname_, 1);
    WritableFile* file;
    Status s = env_->NewWritableFile(manifest, &file);
    if (!s.ok()) {
        return s;
    }
    {
        log::Writer log(file);
        std::string record;
        new_db.EncodeTo(&record);
        s = log.AddRecord(record);
        if (s.ok()) {
            s = file->Sync();
        }
        if (s.ok()) {
            s = file->Close();
        }
    }
    delete file;
    if (s.ok()) {
        // Make "CURRENT" file that points to the new manifest file.
        s = SetCurrentFile(env_, dbname_, 1);
    } else {
        env_->RemoveFile(manifest);
    }
    return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
    if (s->ok() || options_.paranoid_checks) {
        // No change needed
    } else {
        Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
        *s = Status::OK();
    }
}

void DBImpl::RemoveObsoleteFiles() {
    mutex_.AssertHeld();

    if (!bg_error_.ok()) {
        // After a background error, we don't know whether a new version may
        // or may not have been committed, so we cannot safely garbage collect.
        return;
    }

    // Make a set of all of the live files
    std::set<uint64_t> live = pending_outputs_;
    versions_->AddLiveFiles(&live);

    std::vector<std::string> filenames;
    env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
    uint64_t number;
    FileType type;
    std::vector<std::string> files_to_delete;
    for (std::string& filename : filenames) {
        if (ParseFileName(filename, &number, &type)) {
            bool keep = true;
            switch (type) {
                case kLogFile:
                    keep = ((number >= versions_->LogNumber()) ||
                            (number == versions_->PrevLogNumber()));
                    break;
                case kDescriptorFile:
                    // Keep my manifest file, and any newer incarnations'
                    // (in case there is a race that allows other incarnations)
                    keep = (number >= versions_->ManifestFileNumber());
                    break;
                case kTableFile:
                    keep = (live.find(number) != live.end());
                    break;
                case kTempFile:
                    // Any temp files that are currently being written to must
                    // be recorded in pending_outputs_, which is inserted into "live"
                    keep = (live.find(number) != live.end());
                    break;
                case kCurrentFile:
                case kDBLockFile:
                case kInfoLogFile:
                    keep = true;
                    break;
            }

            if (!keep) {
                files_to_delete.push_back(std::move(filename));
                if (type == kTableFile) {
                    table_cache_->Evict(number);
                }
                Log(options_.info_log, "Delete type=%d #%lld\n",
                    static_cast<int>(type),
                    static_cast<unsigned long long>(number));
            }
        }
    }

    // While deleting all files unblock other threads. All files being deleted
    // have unique names which will not collide with newly created files and
    // are therefore safe to delete while allowing other threads to proceed.
    mutex_.Unlock();
    for (const std::string& filename : files_to_delete) {
        env_->RemoveFile(dbname_ + "/" + filename);
    }
    mutex_.Lock();
}

Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
    mutex_.AssertHeld();

    // Ignore error from CreateDir since the creation of the DB is
    // committed only when the descriptor is created, and this directory
    // may already exist from a previous failed creation attempt.
    env_->CreateDir(dbname_);
    assert(db_lock_ == nullptr);
    Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
    if (!s.ok()) {
        return s;
    }

    if (!env_->FileExists(CurrentFileName(dbname_))) {
        if (options_.create_if_missing) {
            Log(options_.info_log, "Creating DB %s since it was missing.",
                dbname_.c_str());
            s = NewDB();
            if (!s.ok()) {
                return s;
            }
        } else {
            return Status::InvalidArgument(
                dbname_, "does not exist (create_if_missing is false)");
        }
    } else {
        if (options_.error_if_exists) {
            return Status::InvalidArgument(dbname_,
                                           "exists (error_if_exists is true)");
        }
    }

    s = versions_->Recover(save_manifest);
    if (!s.ok()) {
        return s;
    }
    SequenceNumber max_sequence(0);

    // Recover from all newer log files than the ones named in the
    // descriptor (new log files may have been added by the previous
    // incarnation without registering them in the descriptor).
    //
    // Note that PrevLogNumber() is no longer used, but we pay
    // attention to it in case we are recovering a database
    // produced by an older version of leveldb.
    const uint64_t min_log = versions_->LogNumber();
    const uint64_t prev_log = versions_->PrevLogNumber();
    std::vector<std::string> filenames;
    s = env_->GetChildren(dbname_, &filenames);
    if (!s.ok()) {
        return s;
    }
    std::set<uint64_t> expected;
    versions_->AddLiveFiles(&expected);
    uint64_t number;
    FileType type;
    std::vector<uint64_t> logs;
    for (size_t i = 0; i < filenames.size(); i++) {
        if (ParseFileName(filenames[i], &number, &type)) {
            expected.erase(number);
            if (type == kLogFile &&
                ((number >= min_log) || (number == prev_log)))
                logs.push_back(number);
        }
    }
    if (!expected.empty()) {
        char buf[50];
        std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                      static_cast<int>(expected.size()));
        return Status::Corruption(buf,
                                  TableFileName(dbname_, *(expected.begin())));
    }

    // Recover in the order in which the logs were generated
    std::sort(logs.begin(), logs.end());
    for (size_t i = 0; i < logs.size(); i++) {
        s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                           &max_sequence);
        if (!s.ok()) {
            return s;
        }

        // The previous incarnation may not have written any MANIFEST
        // records after allocating this log number.  So we manually
        // update the file number allocation counter in VersionSet.
        versions_->MarkFileNumberUsed(logs[i]);
    }

    if (versions_->LastSequence() < max_sequence) {
        versions_->SetLastSequence(max_sequence);
    }

    return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
    struct LogReporter : public log::Reader::Reporter {
        Env* env;
        Logger* info_log;
        const char* fname;
        Status* status;  // null if options_.paranoid_checks==false
        void Corruption(size_t bytes, const Status& s) override {
            Log(info_log, "%s%s: dropping %d bytes; %s",
                (this->status == nullptr ? "(ignoring error) " : ""), fname,
                static_cast<int>(bytes), s.ToString().c_str());
            if (this->status != nullptr && this->status->ok())
                *this->status = s;
        }
    };

    mutex_.AssertHeld();

    // Open the log file
    std::string fname = LogFileName(dbname_, log_number);
    SequentialFile* file;
    Status status = env_->NewSequentialFile(fname, &file);
    if (!status.ok()) {
        MaybeIgnoreError(&status);
        return status;
    }

    // Create the log reader.
    LogReporter reporter;
    reporter.env = env_;
    reporter.info_log = options_.info_log;
    reporter.fname = fname.c_str();
    reporter.status = (options_.paranoid_checks ? &status : nullptr);
    // We intentionally make log::Reader do checksumming even if
    // paranoid_checks==false so that corruptions cause entire commits
    // to be skipped instead of propagating bad information (like overly
    // large sequence numbers).
    log::Reader reader(file, &reporter, true /*checksum*/,
                       0 /*initial_offset*/);
    Log(options_.info_log, "Recovering log #%llu",
        (unsigned long long)log_number);

    // Read all the records and add to a memtable
    std::string scratch;
    Slice record;
    WriteBatch batch;
    int compactions = 0;
    MemTable* mem = nullptr;
    while (reader.ReadRecord(&record, &scratch) && status.ok()) {
        if (record.size() < 12) {
            reporter.Corruption(record.size(),
                                Status::Corruption("log record too small"));
            continue;
        }
        WriteBatchInternal::SetContents(&batch, record);

        if (mem == nullptr) {
            mem = new MemTable(internal_comparator_);
            mem->Ref();
        }
        status = WriteBatchInternal::InsertInto(&batch, mem);
        MaybeIgnoreError(&status);
        if (!status.ok()) {
            break;
        }
        const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                        WriteBatchInternal::Count(&batch) - 1;
        if (last_seq > *max_sequence) {
            *max_sequence = last_seq;
        }

        if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
            compactions++;
            *save_manifest = true;
            status = WriteLevel0Table(mem, edit, nullptr);
            mem->Unref();
            mem = nullptr;
            if (!status.ok()) {
                // Reflect errors immediately so that conditions like full
                // file-systems cause the DB::Open() to fail.
                break;
            }
        }
    }

    delete file;

    // See if we should keep reusing the last log file.
    if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
        assert(logfile_ == nullptr);
        assert(log_ == nullptr);
        assert(mem_ == nullptr);
        uint64_t lfile_size;
        if (env_->GetFileSize(fname, &lfile_size).ok() &&
            env_->NewAppendableFile(fname, &logfile_).ok()) {
            Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
            log_ = new log::Writer(logfile_, lfile_size);
            logfile_number_ = log_number;
            if (mem != nullptr) {
                mem_ = mem;
                mem = nullptr;
            } else {
                // mem can be nullptr if lognum exists but was empty.
                mem_ = new MemTable(internal_comparator_);
                mem_->Ref();
            }
        }
    }

    if (mem != nullptr) {
        // mem did not get reused; compact it.
        if (status.ok()) {
            *save_manifest = true;
            status = WriteLevel0Table(mem, edit, nullptr);
        }
        mem->Unref();
    }

    return status;
}

/**
 * @brief 将内存数据库memtable转化为Level0的sstable，选择一个合适的level，并记录compaction 信息
 *
 * @param mem
 * @param edit
 * @param base
 * @return Status
 */
Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
    //* step1. 预处理，写入sstable前的准备工作
    mutex_.AssertHeld();
    const uint64_t start_micros = env_->NowMicros();  // 记录开始时间
    FileMetaData meta;
    meta.number = versions_->NewFileNumber();  // 生成新的文件编号
    pending_outputs_.insert(meta.number);  // 将当前文件编号加入等待输出的队列中

    Iterator* iter = mem->NewIterator();
    Log(options_.info_log, "Level-0 table #%llu: started",
        (unsigned long long)meta.number);  // 打印日志

    Status s;
    {
        mutex_.Unlock();
        //* step2. 写入sstable：在当前db中遍历imm iter并构建一个0 level的sstable，存储相应的文件信息到meta中
        s = BuildTable(dbname_, env_, options_, table_cache_, iter,
                       &meta);  // 持久化memtable到sstable时不需要加锁?
        mutex_.Lock();
    }

    Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
        (unsigned long long)meta.number, (unsigned long long)meta.file_size,
        s.ToString().c_str());
    delete iter;
    pending_outputs_.erase(meta.number);

    // Note that if file_size is zero, the file has been deleted and
    // should not be added to the manifest.
    int level = 0;
    if (s.ok() && meta.file_size > 0) {
        //* step3. 后处理：如果写入0 level文件成功，则根据其最大key和最小key选择一个合适的level
        const Slice min_user_key = meta.smallest.user_key();
        const Slice max_user_key = meta.largest.user_key();
        if (base != nullptr) {
            level =
                base->PickLevelForMemTableOutput(min_user_key, max_user_key);
        }
        //* 将sstable文件信息存储到VersionEdit对象中
        edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                      meta.largest);
    }

    //* step4. 记录compaction状态
    CompactionStats stats;
    stats.micros =
        env_->NowMicros() - start_micros;  // 记录当前compaction所花费的微秒数
    stats.bytes_written = meta.file_size;
    stats_[level].Add(stats);
    return s;
}

void DBImpl::CompactMemTable() {
    mutex_.AssertHeld();
    assert(imm_ != nullptr);

    // Save the contents of the memtable as a new Table
    //* step1. 压缩imm，进行minor compaction，并记录相应的信息到compactionStats和versionEdit中
    VersionEdit edit;
    Version* base = versions_->current();
    base->Ref();
    Status s = WriteLevel0Table(imm_, &edit, base);
    base->Unref();

    if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
        s = Status::IOError("Deleting DB during memtable compaction");
    }

    // Replace immutable memtable with the generated Table
    if (s.ok()) {
        edit.SetPrevLogNumber(0);
        edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
        s = versions_->LogAndApply(&edit, &mutex_);
    }

    if (s.ok()) {
        // Commit to the new state
        //* 将imm置为空
        imm_->Unref();
        imm_ = nullptr;
        has_imm_.store(false, std::memory_order_release);
        RemoveObsoleteFiles();
    } else {
        RecordBackgroundError(s);
    }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
    int max_level_with_files = 1;
    {
        MutexLock l(&mutex_);
        Version* base = versions_->current();
        for (int level = 1; level < config::kNumLevels; level++) {
            if (base->OverlapInLevel(level, begin, end)) {
                max_level_with_files = level;
            }
        }
    }
    TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
    for (int level = 0; level < max_level_with_files; level++) {
        TEST_CompactRange(level, begin, end);
    }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
    assert(level >= 0);
    assert(level + 1 < config::kNumLevels);

    InternalKey begin_storage, end_storage;

    ManualCompaction manual;
    manual.level = level;
    manual.done = false;
    if (begin == nullptr) {
        manual.begin = nullptr;
    } else {
        begin_storage =
            InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
        manual.begin = &begin_storage;
    }
    if (end == nullptr) {
        manual.end = nullptr;
    } else {
        end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
        manual.end = &end_storage;
    }

    MutexLock l(&mutex_);
    while (!manual.done && !shutting_down_.load(std::memory_order_acquire) &&
           bg_error_.ok()) {
        if (manual_compaction_ == nullptr) {  // Idle
            manual_compaction_ = &manual;
            MaybeScheduleCompaction();
        } else {  // Running either my compaction or another compaction.
            background_work_finished_signal_.Wait();
        }
    }
    if (manual_compaction_ == &manual) {
        // Cancel my manual compaction since we aborted early for some reason.
        manual_compaction_ = nullptr;
    }
}

Status DBImpl::TEST_CompactMemTable() {
    // nullptr batch means just wait for earlier writes to be done
    Status s = Write(WriteOptions(), nullptr);
    if (s.ok()) {
        // Wait until the compaction completes
        MutexLock l(&mutex_);
        while (imm_ != nullptr && bg_error_.ok()) {
            background_work_finished_signal_.Wait();
        }
        if (imm_ != nullptr) {
            s = bg_error_;
        }
    }
    return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
    mutex_.AssertHeld();
    if (bg_error_.ok()) {
        bg_error_ = s;
        background_work_finished_signal_.SignalAll();
    }
}

void DBImpl::MaybeScheduleCompaction() {
    mutex_.AssertHeld();
    if (background_compaction_scheduled_) {
        // Already scheduled
    } else if (shutting_down_.load(std::memory_order_acquire)) {
        // DB is being deleted; no more background compactions
    } else if (!bg_error_.ok()) {
        // Already got an error; no more changes
    } else if (imm_ == nullptr && manual_compaction_ == nullptr &&
               !versions_->NeedsCompaction()) {
        // No work to be done
    } else {
        background_compaction_scheduled_ = true;
        env_->Schedule(&DBImpl::BGWork, this);
    }
}

void DBImpl::BGWork(void* db) {
    reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall() {
    MutexLock l(&mutex_);
    assert(background_compaction_scheduled_);
    if (shutting_down_.load(std::memory_order_acquire)) {
        // No more background work when shutting down.
    } else if (!bg_error_.ok()) {
        // No more background work after a background error.
    } else {
        //* 进行compaction
        //* 全程通过mutex进行保护
        BackgroundCompaction();
    }

    background_compaction_scheduled_ = false;

    // Previous compaction may have produced too many files in a level,
    // so reschedule another compaction if needed.
    //* 先前的compaction可能造成一个level中存在太多的文件，如果是则重新调度compactionn
    MaybeScheduleCompaction();

    //* 通知所有等待后台任务的条件变量
    background_work_finished_signal_.SignalAll();
}

void DBImpl::BackgroundCompaction() {
    mutex_.AssertHeld();

    if (imm_ != nullptr) {
        CompactMemTable();
        return;
    }

    Compaction* c;
    bool is_manual = (manual_compaction_ != nullptr);
    InternalKey manual_end;
    if (is_manual) {
        ManualCompaction* m = manual_compaction_;
        c = versions_->CompactRange(m->level, m->begin, m->end);
        m->done = (c == nullptr);
        if (c != nullptr) {
            manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
        }
        Log(options_.info_log,
            "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
            m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
            (m->end ? m->end->DebugString().c_str() : "(end)"),
            (m->done ? "(end)" : manual_end.DebugString().c_str()));
    } else {
        c = versions_->PickCompaction();
    }

    Status status;
    if (c == nullptr) {
        // Nothing to do
    } else if (!is_manual && c->IsTrivialMove()) {
        // Move file to next level
        assert(c->num_input_files(0) == 1);
        FileMetaData* f = c->input(0, 0);
        c->edit()->RemoveFile(c->level(), f->number);
        c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest,
                           f->largest);
        status = versions_->LogAndApply(c->edit(), &mutex_);
        if (!status.ok()) {
            RecordBackgroundError(status);
        }
        VersionSet::LevelSummaryStorage tmp;
        Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
            static_cast<unsigned long long>(f->number), c->level() + 1,
            static_cast<unsigned long long>(f->file_size),
            status.ToString().c_str(), versions_->LevelSummary(&tmp));
    } else {
        CompactionState* compact = new CompactionState(c);
        status = DoCompactionWork(compact);
        if (!status.ok()) {
            RecordBackgroundError(status);
        }
        CleanupCompaction(compact);
        c->ReleaseInputs();
        RemoveObsoleteFiles();
    }
    delete c;

    if (status.ok()) {
        // Done
    } else if (shutting_down_.load(std::memory_order_acquire)) {
        // Ignore compaction errors found during shutting down
    } else {
        Log(options_.info_log, "Compaction error: %s",
            status.ToString().c_str());
    }

    if (is_manual) {
        ManualCompaction* m = manual_compaction_;
        if (!status.ok()) {
            m->done = true;
        }
        if (!m->done) {
            // We only compacted part of the requested range.  Update *m
            // to the range that is left to be compacted.
            m->tmp_storage = manual_end;
            m->begin = &m->tmp_storage;
        }
        manual_compaction_ = nullptr;
    }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
    mutex_.AssertHeld();
    if (compact->builder != nullptr) {
        // May happen if we get a shutdown call in the middle of compaction
        compact->builder->Abandon();
        delete compact->builder;
    } else {
        assert(compact->outfile == nullptr);
    }
    delete compact->outfile;
    for (size_t i = 0; i < compact->outputs.size(); i++) {
        const CompactionState::Output& out = compact->outputs[i];
        pending_outputs_.erase(out.number);
    }
    delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
    assert(compact != nullptr);
    assert(compact->builder == nullptr);
    uint64_t file_number;
    {
        mutex_.Lock();
        file_number = versions_->NewFileNumber();
        pending_outputs_.insert(file_number);
        CompactionState::Output out;
        out.number = file_number;
        out.smallest.Clear();
        out.largest.Clear();
        compact->outputs.push_back(out);
        mutex_.Unlock();
    }

    // Make the output file
    std::string fname = TableFileName(dbname_, file_number);
    Status s = env_->NewWritableFile(fname, &compact->outfile);
    if (s.ok()) {
        compact->builder = new TableBuilder(options_, compact->outfile);
    }
    return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
    assert(compact != nullptr);
    assert(compact->outfile != nullptr);
    assert(compact->builder != nullptr);

    const uint64_t output_number = compact->current_output()->number;
    assert(output_number != 0);

    // Check for iterator errors
    Status s = input->status();
    const uint64_t current_entries = compact->builder->NumEntries();
    if (s.ok()) {
        s = compact->builder->Finish();
    } else {
        compact->builder->Abandon();
    }
    const uint64_t current_bytes = compact->builder->FileSize();
    compact->current_output()->file_size = current_bytes;
    compact->total_bytes += current_bytes;
    delete compact->builder;
    compact->builder = nullptr;

    // Finish and check for file errors
    if (s.ok()) {
        s = compact->outfile->Sync();
    }
    if (s.ok()) {
        s = compact->outfile->Close();
    }
    delete compact->outfile;
    compact->outfile = nullptr;

    if (s.ok() && current_entries > 0) {
        // Verify that the table is usable
        Iterator* iter = table_cache_->NewIterator(ReadOptions(), output_number,
                                                   current_bytes);
        s = iter->status();
        delete iter;
        if (s.ok()) {
            Log(options_.info_log,
                "Generated table #%llu@%d: %lld keys, %lld bytes",
                (unsigned long long)output_number, compact->compaction->level(),
                (unsigned long long)current_entries,
                (unsigned long long)current_bytes);
        }
    }
    return s;
}

Status DBImpl::InstallCompactionResults(CompactionState* compact) {
    mutex_.AssertHeld();
    Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
        compact->compaction->num_input_files(0), compact->compaction->level(),
        compact->compaction->num_input_files(1),
        compact->compaction->level() + 1,
        static_cast<long long>(compact->total_bytes));

    // Add compaction outputs
    compact->compaction->AddInputDeletions(compact->compaction->edit());
    const int level = compact->compaction->level();
    for (size_t i = 0; i < compact->outputs.size(); i++) {
        const CompactionState::Output& out = compact->outputs[i];
        compact->compaction->edit()->AddFile(
            level + 1, out.number, out.file_size, out.smallest, out.largest);
    }
    return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
    const uint64_t start_micros = env_->NowMicros();
    int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

    Log(options_.info_log, "Compacting %d@%d + %d@%d files",
        compact->compaction->num_input_files(0), compact->compaction->level(),
        compact->compaction->num_input_files(1),
        compact->compaction->level() + 1);

    assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
    assert(compact->builder == nullptr);
    assert(compact->outfile == nullptr);
    if (snapshots_.empty()) {
        compact->smallest_snapshot = versions_->LastSequence();
    } else {
        compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
    }

    Iterator* input = versions_->MakeInputIterator(compact->compaction);

    // Release mutex while we're actually doing the compaction work
    mutex_.Unlock();

    input->SeekToFirst();
    Status status;
    ParsedInternalKey ikey;
    std::string current_user_key;
    bool has_current_user_key = false;
    SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
    while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {
        // Prioritize immutable compaction work
        if (has_imm_.load(std::memory_order_relaxed)) {
            const uint64_t imm_start = env_->NowMicros();
            mutex_.Lock();
            if (imm_ != nullptr) {
                CompactMemTable();
                // Wake up MakeRoomForWrite() if necessary.
                background_work_finished_signal_.SignalAll();
            }
            mutex_.Unlock();
            imm_micros += (env_->NowMicros() - imm_start);
        }

        Slice key = input->key();
        if (compact->compaction->ShouldStopBefore(key) &&
            compact->builder != nullptr) {
            status = FinishCompactionOutputFile(compact, input);
            if (!status.ok()) {
                break;
            }
        }

        // Handle key/value, add to state, etc.
        bool drop = false;
        if (!ParseInternalKey(key, &ikey)) {
            // Do not hide error keys
            current_user_key.clear();
            has_current_user_key = false;
            last_sequence_for_key = kMaxSequenceNumber;
        } else {
            if (!has_current_user_key ||
                user_comparator()->Compare(ikey.user_key,
                                           Slice(current_user_key)) != 0) {
                // First occurrence of this user key
                current_user_key.assign(ikey.user_key.data(),
                                        ikey.user_key.size());
                has_current_user_key = true;
                last_sequence_for_key = kMaxSequenceNumber;
            }

            if (last_sequence_for_key <= compact->smallest_snapshot) {
                // Hidden by an newer entry for same user key
                drop = true;  // (A)
            } else if (ikey.type == kTypeDeletion &&
                       ikey.sequence <= compact->smallest_snapshot &&
                       compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
                // For this user key:
                // (1) there is no data in higher levels
                // (2) data in lower levels will have larger sequence numbers
                // (3) data in layers that are being compacted here and have
                //     smaller sequence numbers will be dropped in the next
                //     few iterations of this loop (by rule (A) above).
                // Therefore this deletion marker is obsolete and can be dropped.
                drop = true;
            }

            last_sequence_for_key = ikey.sequence;
        }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

        if (!drop) {
            // Open output file if necessary
            if (compact->builder == nullptr) {
                status = OpenCompactionOutputFile(compact);
                if (!status.ok()) {
                    break;
                }
            }
            if (compact->builder->NumEntries() == 0) {
                compact->current_output()->smallest.DecodeFrom(key);
            }
            compact->current_output()->largest.DecodeFrom(key);
            compact->builder->Add(key, input->value());

            // Close output file if it is big enough
            if (compact->builder->FileSize() >=
                compact->compaction->MaxOutputFileSize()) {
                status = FinishCompactionOutputFile(compact, input);
                if (!status.ok()) {
                    break;
                }
            }
        }

        input->Next();
    }

    if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
        status = Status::IOError("Deleting DB during compaction");
    }
    if (status.ok() && compact->builder != nullptr) {
        status = FinishCompactionOutputFile(compact, input);
    }
    if (status.ok()) {
        status = input->status();
    }
    delete input;
    input = nullptr;

    CompactionStats stats;
    stats.micros = env_->NowMicros() - start_micros - imm_micros;
    for (int which = 0; which < 2; which++) {
        for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
            stats.bytes_read += compact->compaction->input(which, i)->file_size;
        }
    }
    for (size_t i = 0; i < compact->outputs.size(); i++) {
        stats.bytes_written += compact->outputs[i].file_size;
    }

    mutex_.Lock();
    stats_[compact->compaction->level() + 1].Add(stats);

    if (status.ok()) {
        status = InstallCompactionResults(compact);
    }
    if (!status.ok()) {
        RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
    return status;
}

namespace {

struct IterState {
    port::Mutex* const mu;
    Version* const version GUARDED_BY(mu);
    MemTable* const mem GUARDED_BY(mu);
    MemTable* const imm GUARDED_BY(mu);

    IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm,
              Version* version)
        : mu(mutex), version(version), mem(mem), imm(imm) {}
};

static void CleanupIteratorState(void* arg1, void* arg2) {
    IterState* state = reinterpret_cast<IterState*>(arg1);
    state->mu->Lock();
    state->mem->Unref();
    if (state->imm != nullptr) state->imm->Unref();
    state->version->Unref();
    state->mu->Unlock();
    delete state;
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
    mutex_.Lock();
    *latest_snapshot = versions_->LastSequence();

    // Collect together all needed child iterators
    std::vector<Iterator*> list;
    list.push_back(mem_->NewIterator());
    mem_->Ref();
    if (imm_ != nullptr) {
        list.push_back(imm_->NewIterator());
        imm_->Ref();
    }

    //* 将当前version的数据库sstable迭代器添加到list中
    versions_->current()->AddIterators(options, &list);

    Iterator* internal_iter =
        NewMergingIterator(&internal_comparator_, &list[0], list.size());
    versions_->current()->Ref();

    IterState* cleanup =
        new IterState(&mutex_, mem_, imm_, versions_->current());
    internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

    *seed = ++seed_;
    mutex_.Unlock();
    return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
    SequenceNumber ignored;
    uint32_t ignored_seed;
    return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
    MutexLock l(&mutex_);
    return versions_->MaxNextLevelOverlappingBytes();
}

/**
 * @brief 从数据库中获取查询的键值对，查询顺序: mem -> imm -> sstable
 *
 * @param options
 * @param key
 * @param value
 * @return Status
 */
Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
    Status s;
    MutexLock l(&mutex_);
    SequenceNumber snapshot;
    if (options.snapshot != nullptr) {
        snapshot = static_cast<const SnapshotImpl*>(options.snapshot)
                       ->sequence_number();
    } else {
        snapshot = versions_->LastSequence();
    }

    MemTable* mem = mem_;
    MemTable* imm = imm_;
    Version* current = versions_->current();
    mem->Ref();
    if (imm != nullptr) imm->Ref();
    current->Ref();

    bool have_stat_update = false;
    Version::GetStats stats;

    // Unlock while reading from files and memtables
    {
        mutex_.Unlock();
        // First look in the memtable, then in the immutable memtable (if any).
        LookupKey lkey(key, snapshot);
        if (mem->Get(lkey, value, &s)) {
            // Done
        } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
            // Done
        } else {
            s = current->Get(options, lkey, value, &stats);
            have_stat_update = true;
        }
        mutex_.Lock();
    }

    if (have_stat_update && current->UpdateStats(stats)) {
        MaybeScheduleCompaction();
    }
    mem->Unref();
    if (imm != nullptr) imm->Unref();
    current->Unref();
    return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
    SequenceNumber latest_snapshot;
    uint32_t seed;
    Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
    return NewDBIterator(
        this, user_comparator(), iter,
        (options.snapshot != nullptr
             ? static_cast<const SnapshotImpl*>(options.snapshot)
                   ->sequence_number()
             : latest_snapshot),
        seed);
}

void DBImpl::RecordReadSample(Slice key) {
    MutexLock l(&mutex_);
    if (versions_->current()->RecordReadSample(key)) {
        MaybeScheduleCompaction();
    }
}

const Snapshot* DBImpl::GetSnapshot() {
    MutexLock l(&mutex_);
    return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
    MutexLock l(&mutex_);
    snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
    return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
    return DB::Delete(options, key);
}

/**
 * @brief 重载自DB的虚函数，进行写入
 *
 * @param options
 * @param updates
 * @return Status
 */
Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
    /**
     * @brief 管理写入状态的结构体
     * status 返回状态
     * batch 当前写入批次数据
     * done 完成标志
     * cond 条件变量
     */
    Writer w(&mutex_);
    w.batch = updates;
    w.sync = options.sync;  // 写入模式：同步/异步
    w.done = false;

    //========== 临界区 begin ==========
    MutexLock l(&mutex_);  //? 类似于thread_guard，进行加锁保护

    writers_.push_back(&w);  // 将当前写入writer放入等待队列中

    //* 当前任务w未被执行(!w.done)且任务未被调度(&w != writers_.front())时，循环wait等待唤醒
    //! w.done在合并操作时会被置为true，即被其他线程执行完
    while (!w.done && &w != writers_.front()) {
        // wait解锁并进行等待signal，在这段时间里，writers_队列可能添加进新的元素
        //========== 临界区 end ==========
        w.cv.Wait();  // writer和MutexLock用的是同一个互斥锁，其中writer的cond使用adopt_lock的形式，防止重复加锁
    }

    //========== 临界区 begin ==========
    //* 情况1. 所加入的writer被其他线程处理了，返回其处理结果status
    //* 情况2. 所加入的writer到达队头，进行后续处理
    if (w.done) {
        return w.status;
    }

    // May temporarily unlock and wait.
    //* 写入前检查: 后台compaction错误检查、mem空间检查、执行mem和imm切换、通知后台compaction
    Status status = MakeRoomForWrite(updates == nullptr);

    //* 获取上一个版本号
    uint64_t last_sequence = versions_->LastSequence();

    Writer* last_writer = &w;
    if (status.ok() &&
        updates != nullptr) {  // nullptr batch is for compactions

        //* 将从w到last_writer的batch进行组合，成为一个大的write_batch
        //* writer_batch包括从w->batch 到 last_writer->batch的组合
        WriteBatch* write_batch = BuildBatchGroup(&last_writer);

        //* 为合并后的write_batch设置序列号+1，即batch中至少一条record
        //? 一个writebatch内的所有操作共享一个版本号sequence number
        WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);

        //* 将sequence加上此次合并batch的record条数
        last_sequence += WriteBatchInternal::Count(write_batch);

        // Add to log and apply to memtable.  We can release the lock
        // during this phase since &w is currently responsible for logging
        // and protects against concurrent loggers and concurrent writes
        // into mem_.
        //? 进行日志的写入logging和内存表的写入apply to memtable
        //? 此时对于log和memtable的写入，Writer对象已经持有了必要的锁，所以不需要mutex_进行保护
        //========== 临界区 end ==========
        {
            //* step1. 解锁
            mutex_.Unlock();

            //* step2. 日志写
            status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
            bool sync_error = false;
            // 如果开启了同步模式，则调用sync进行同步，持久化到磁盘中
            if (status.ok() && options.sync) {
                status = logfile_->Sync();
                if (!status.ok()) {
                    sync_error = true;
                }
            }

            //* step3. memtable写
            if (status.ok()) {
                status = WriteBatchInternal::InsertInto(write_batch, mem_);
            }

            //* step4. 加锁
            mutex_.Lock();

            //========== 临界区 begin ==========
            //* step5. 处理同步错误
            if (sync_error) {
                // The state of the log file is indeterminate: the log record we
                // just added may or may not show up when the DB is re-opened.
                // So we force the DB into a mode where all future writes fail.
                //* 如果日志写发生同步错误，此时报告bg error
                RecordBackgroundError(status);
            }
        }
        //* write_batch是在tmp_batch中组合的，清空tmp_batch为下一次组合做准备
        if (write_batch == tmp_batch_) tmp_batch_->Clear();

        //* 更新版本号
        versions_->SetLastSequence(last_sequence);
    }

    //* 将被合并的writer从队列中取出，并置为状态done，然后通知相对的condvar
    //* 在last_writer之前，可能有多个writer由于BuildBatchGroup合并被执行，需要设置其状态
    while (true) {
        Writer* ready = writers_.front();
        writers_.pop_front();
        // 被合并的writer
        if (ready != &w) {
            ready->status = status;
            ready->done = true;
            ready->cv.Signal();
        }
        if (ready == last_writer) break;
    }

    // Notify new head of write queue
    //* 通知对头的writer可以开始进行写入了
    if (!writers_.empty()) {
        writers_.front()->cv.Signal();
    }

    return status;
    //========== 临界区 end ==========
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
/**
 * @brief 将多个任务中的writebatch组合成一个大的writebatch，进行后续的插入操作
 * 此时BuildBatchGroup整个函数处于mutex_保护的状态下
 * @param last_writer
 * @return WriteBatch*
 */
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
    mutex_.AssertHeld();
    assert(!writers_.empty());
    Writer* first = writers_.front();
    WriteBatch* result = first->batch;  // batch结果存放的位置
    assert(result != nullptr);

    size_t size = WriteBatchInternal::ByteSize(first->batch);

    // Allow the group to grow up to a maximum size, but if the
    // original write is small, limit the growth so we do not slow
    // down the small write too much.
    //? 允许的batchgroup最大为1M
    size_t max_size = 1 << 20;  // 2^20 = 1M
    //? 当队头的writebatch太小时(size <= 128K)，限制max size，防止等待太多的small batch进行组合
    if (size <= (128 << 10)) {
        max_size = size + (128 << 10);
    }

    *last_writer = first;  // 记录last_writer

    // 从第二个writer开始合并
    std::deque<Writer*>::iterator iter = writers_.begin();
    ++iter;  // Advance past "first"
    for (; iter != writers_.end(); ++iter) {
        Writer* w = *iter;
        //? 同步模式不同，则不进行组合
        if (w->sync && !first->sync) {
            // Do not include a sync write into a batch handled by a non-sync write.
            break;
        }

        if (w->batch != nullptr) {
            size += WriteBatchInternal::ByteSize(w->batch);
            if (size > max_size) {
                // Do not make batch too big
                //? 避免单次写入的数据量过大
                break;
            }

            // Append to *result
            //? 在tmp_batch内进行append，而不是在first->batch内进行append，从而保证了数据的安全性
            if (result == first->batch) {
                // Switch to temporary batch instead of disturbing caller's batch
                result = tmp_batch_;
                assert(WriteBatchInternal::Count(result) ==
                       0);  // 保证tmp_batch是clear过的
                WriteBatchInternal::Append(result, first->batch);
            }
            WriteBatchInternal::Append(result, w->batch);
        }
        *last_writer = w;  // 记录组合的最后一个writer
    }
    return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
/**
 * @brief 写入检查，留出足够的写入空间
 *
 * @param force = (updates == nullptr)
 * @return Status
 */
Status DBImpl::MakeRoomForWrite(bool force) {
    mutex_.AssertHeld();
    assert(!writers_.empty());

    //* 当前write_batch不为空时，允许延迟
    bool allow_delay = !force;

    Status s;
    while (true) {
        if (!bg_error_.ok()) {
            //? 情况1. 后台任务有无错误？有则直接返回(compaction由后台线程执行)
            // Yield previous error
            s = bg_error_;
            break;
        } else if (allow_delay && versions_->NumLevelFiles(0) >=
                                      config::kL0_SlowdownWritesTrigger) {
            //? 情况2. level0的文件数目如果超过设置阈值，则等待1000micros再执行，防止level0文件过多
            // We are getting close to hitting a hard limit on the number of
            // L0 files.  Rather than delaying a single write by several
            // seconds when we hit the hard limit, start delaying each
            // individual write by 1ms to reduce latency variance.  Also,
            // this delay hands over some CPU to the compaction thread in
            // case it is sharing the same core as the writer.
            mutex_.Unlock();
            env_->SleepForMicroseconds(1000);
            allow_delay = false;  // Do not delay a single write more than once
            mutex_.Lock();
        } else if (!force && (mem_->ApproximateMemoryUsage() <=
                              options_.write_buffer_size)) {
            //? 情况3. 如果memtable内存可用可直接插入，则直接返回
            // There is room in current memtable
            break;
        } else if (imm_ != nullptr) {
            //? 情况4. 此时memtable已满，需要切换为immutable memtable。等待后台线程将imm进行compaction，到达level0
            // We have filled up the current memtable, but the previous
            // one is still being compacted, so we wait.
            Log(options_.info_log, "Current memtable full; waiting...\n");
            background_work_finished_signal_.Wait();
        } else if (versions_->NumLevelFiles(0) >=
                   config::kL0_StopWritesTrigger) {
            //? 情况5. 此时旧的imm已经到达level0，如果此时level0文件数目太多，也需要等待
            // There are too many level-0 files.
            Log(options_.info_log, "Too many L0 files; waiting...\n");
            background_work_finished_signal_.Wait();
        } else {
            //? 情况6. 此时imm已经compaction到达磁盘，level0的文件数目也符合要求，则将mem转为imm，并启动后台的compaction；并生成新的mem和log用于写入
            // Attempt to switch to a new memtable and trigger compaction of old
            //* 新建一个memtable并执行旧的memtable的compaction
            assert(versions_->PrevLogNumber() == 0);
            uint64_t new_log_number = versions_->NewFileNumber();
            WritableFile* lfile = nullptr;
            s = env_->NewWritableFile(LogFileName(dbname_, new_log_number),
                                      &lfile);
            if (!s.ok()) {
                // Avoid chewing through file number space in a tight loop.
                versions_->ReuseFileNumber(new_log_number);
                break;
            }

            delete log_;

            s = logfile_->Close();
            if (!s.ok()) {
                // We may have lost some data written to the previous log file.
                // Switch to the new log file anyway, but record as a background
                // error so we do not attempt any more writes.
                //
                // We could perhaps attempt to save the memtable corresponding
                // to log file and suppress the error if that works, but that
                // would add more complexity in a critical code path.
                RecordBackgroundError(s);
            }
            delete logfile_;

            //* 新建logfile、新建memtable、将memtable传给immutable memtable并执行compaction
            logfile_ = lfile;
            logfile_number_ = new_log_number;
            log_ = new log::Writer(lfile);

            imm_ = mem_;
            has_imm_.store(true, std::memory_order_release);

            mem_ = new MemTable(internal_comparator_);
            mem_->Ref();
            force = false;  // Do not force another compaction if have room

            MaybeScheduleCompaction();
        }
    }
    return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
    value->clear();

    MutexLock l(&mutex_);
    Slice in = property;
    Slice prefix("leveldb.");
    if (!in.starts_with(prefix)) return false;
    in.remove_prefix(prefix.size());

    if (in.starts_with("num-files-at-level")) {
        in.remove_prefix(strlen("num-files-at-level"));
        uint64_t level;
        bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
        if (!ok || level >= config::kNumLevels) {
            return false;
        } else {
            char buf[100];
            std::snprintf(buf, sizeof(buf), "%d",
                          versions_->NumLevelFiles(static_cast<int>(level)));
            *value = buf;
            return true;
        }
    } else if (in == "stats") {
        char buf[200];
        std::snprintf(buf, sizeof(buf),
                      "                               Compactions\n"
                      "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                      "--------------------------------------------------\n");
        value->append(buf);
        for (int level = 0; level < config::kNumLevels; level++) {
            int files = versions_->NumLevelFiles(level);
            if (stats_[level].micros > 0 || files > 0) {
                std::snprintf(buf, sizeof(buf),
                              "%3d %8d %8.0f %9.0f %8.0f %9.0f\n", level, files,
                              versions_->NumLevelBytes(level) / 1048576.0,
                              stats_[level].micros / 1e6,
                              stats_[level].bytes_read / 1048576.0,
                              stats_[level].bytes_written / 1048576.0);
                value->append(buf);
            }
        }
        return true;
    } else if (in == "sstables") {
        *value = versions_->current()->DebugString();
        return true;
    } else if (in == "approximate-memory-usage") {
        size_t total_usage = options_.block_cache->TotalCharge();
        if (mem_) {
            total_usage += mem_->ApproximateMemoryUsage();
        }
        if (imm_) {
            total_usage += imm_->ApproximateMemoryUsage();
        }
        char buf[50];
        std::snprintf(buf, sizeof(buf), "%llu",
                      static_cast<unsigned long long>(total_usage));
        value->append(buf);
        return true;
    }

    return false;
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
    // TODO(opt): better implementation
    MutexLock l(&mutex_);
    Version* v = versions_->current();
    v->Ref();

    for (int i = 0; i < n; i++) {
        // Convert user_key into a corresponding internal key.
        InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
        InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
        uint64_t start = versions_->ApproximateOffsetOf(v, k1);
        uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
        sizes[i] = (limit >= start ? limit - start : 0);
    }

    v->Unref();
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
    WriteBatch batch;
    batch.Put(key, value);
    return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
    WriteBatch batch;
    batch.Delete(key);
    return Write(opt, &batch);
}

DB::~DB() = default;

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
    *dbptr = nullptr;

    DBImpl* impl = new DBImpl(options, dbname);
    impl->mutex_.Lock();
    VersionEdit edit;
    // Recover handles create_if_missing, error_if_exists
    bool save_manifest = false;
    Status s = impl->Recover(&edit, &save_manifest);
    if (s.ok() && impl->mem_ == nullptr) {
        // Create new log and a corresponding memtable.
        uint64_t new_log_number = impl->versions_->NewFileNumber();
        WritableFile* lfile;
        //* 打开一个logfile文件
        s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                         &lfile);
        if (s.ok()) {
            edit.SetLogNumber(new_log_number);
            impl->logfile_ = lfile;
            impl->logfile_number_ = new_log_number;
            impl->log_ = new log::Writer(lfile);  // log writer
            impl->mem_ = new MemTable(impl->internal_comparator_);  // memtable
            impl->mem_->Ref();  // 增加引用计数
        }
    }
    if (s.ok() && save_manifest) {
        edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
        edit.SetLogNumber(impl->logfile_number_);
        s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
    }
    if (s.ok()) {
        impl->RemoveObsoleteFiles();
        impl->MaybeScheduleCompaction();
    }
    impl->mutex_.Unlock();

    //* 判断是否打开db成功，成功则返回db指针，失败则删除impl并返回错误状态
    if (s.ok()) {
        assert(impl->mem_ != nullptr);
        *dbptr = impl;
    } else {
        delete impl;
    }
    return s;
}

Snapshot::~Snapshot() = default;

Status DestroyDB(const std::string& dbname, const Options& options) {
    Env* env = options.env;
    std::vector<std::string> filenames;
    Status result = env->GetChildren(dbname, &filenames);
    if (!result.ok()) {
        // Ignore error in case directory does not exist
        return Status::OK();
    }

    FileLock* lock;
    const std::string lockname = LockFileName(dbname);
    result = env->LockFile(lockname, &lock);
    if (result.ok()) {
        uint64_t number;
        FileType type;
        for (size_t i = 0; i < filenames.size(); i++) {
            if (ParseFileName(filenames[i], &number, &type) &&
                type != kDBLockFile) {  // Lock file will be deleted at end
                Status del = env->RemoveFile(dbname + "/" + filenames[i]);
                if (result.ok() && !del.ok()) {
                    result = del;
                }
            }
        }
        env->UnlockFile(lock);  // Ignore error since state is already gone
        env->RemoveFile(lockname);
        env->RemoveDir(
            dbname);  // Ignore error in case dir contains other files
    }
    return result;
}

}  // namespace leveldb
