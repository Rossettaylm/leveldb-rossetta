// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <cassert>
#include <cstdio>

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

static size_t TargetFileSize(const Options* options) {
    return options->max_file_size;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static int64_t MaxGrandParentOverlapBytes(const Options* options) {
    return 10 * TargetFileSize(options);
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
    return 25 * TargetFileSize(options);
}

static double MaxBytesForLevel(const Options* options, int level) {
    // Note: the result for level zero is not really used since we set
    // the level-0 compaction threshold based on number of files.

    // Result for both level-0 and level-1
    double result = 10. * 1048576.0;
    while (level > 1) {
        result *= 10;
        level--;
    }
    return result;
}

static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
    // We could vary per level to reduce number of files?
    return TargetFileSize(options);
}

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
    int64_t sum = 0;
    for (size_t i = 0; i < files.size(); i++) {
        sum += files[i]->file_size;
    }
    return sum;
}

Version::~Version() {
    assert(refs_ == 0);

    // Remove from linked list
    prev_->next_ = next_;
    next_->prev_ = prev_;

    // Drop references to files
    for (int level = 0; level < config::kNumLevels; level++) {
        for (size_t i = 0; i < files_[level].size(); i++) {
            FileMetaData* f = files_[level][i];
            assert(f->refs > 0);
            f->refs--;
            if (f->refs <= 0) {
                delete f;
            }
        }
    }
}

/**
 * @brief 二分搜索查找files中的文件，根据最大key进行匹配
 *
 * @param icmp
 * @param files
 * @param key
 * @return int
 */
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key) {
    uint32_t left = 0;
    uint32_t right = files.size();
    while (left < right) {
        uint32_t mid = (left + right) / 2;
        const FileMetaData* f = files[mid];
        if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
            // Key at "mid.largest" is < "target".  Therefore all
            // files at or before "mid" are uninteresting.
            left = mid + 1;
        } else {
            // Key at "mid.largest" is >= "target".  Therefore all files
            // after "mid" are uninteresting.
            right = mid;
        }
    }
    return right;
}

static bool AfterFile(const Comparator* ucmp, const Slice* user_key,
                      const FileMetaData* f) {
    // null user_key occurs before all keys and is therefore never after *f
    return (user_key != nullptr &&
            ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

static bool BeforeFile(const Comparator* ucmp, const Slice* user_key,
                       const FileMetaData* f) {
    // null user_key occurs after all keys and is therefore never before *f
    return (user_key != nullptr &&
            ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

/**
 * @brief 判断给定的范围是否和文件有overlap
 *
 * @param icmp
 * @param disjoint_sorted_files 排序标志，判定是否需要遍历所有文件
 * @param files 给定的文件列表
 * @param smallest_user_key 给定最小key
 * @param largest_user_key 给定最大key
 * @return true 有overlap
 * @return false
 */
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key) {
    const Comparator* ucmp = icmp.user_comparator();
    if (!disjoint_sorted_files) {
        // Need to check against all files
        // 遍历所有file并检查[smallest, largest]范围是否和file有overlap
        for (size_t i = 0; i < files.size(); i++) {
            const FileMetaData* f = files[i];
            if (AfterFile(ucmp, smallest_user_key, f) ||
                BeforeFile(ucmp, largest_user_key, f)) {
                // No overlap
            } else {
                return true;  // Overlap
            }
        }
        return false;
    }

    // Binary search over file list
    // 进行二分搜索
    uint32_t index = 0;
    if (smallest_user_key != nullptr) {
        // Find the earliest possible internal key for smallest_user_key
        InternalKey small_key(*smallest_user_key, kMaxSequenceNumber,
                              kValueTypeForSeek);
        index = FindFile(icmp, files, small_key.Encode());
    }

    if (index >= files.size()) {
        // beginning of range is after all files, so no overlap.
        return false;
    }

    return !BeforeFile(ucmp, largest_user_key, files[index]);
}

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.
// For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
//* 用于遍历一层level中的所有file信息
//* 内部迭代器，通过给定的version/level的pair对，给出当前level中的files信息
//* 对于一个给出的entry，key()为file文件中的最大key，value()为16-byte的值，包含file number和file size的编码
class Version::LevelFileNumIterator : public Iterator {
  public:
    LevelFileNumIterator(const InternalKeyComparator& icmp,
                         const std::vector<FileMetaData*>* flist)
        : icmp_(icmp),
          flist_(flist),
          index_(flist->size()) {  // Marks as invalid
    }
    bool Valid() const override { return index_ < flist_->size(); }
    void Seek(const Slice& target) override {
        index_ = FindFile(icmp_, *flist_, target);
    }
    void SeekToFirst() override { index_ = 0; }
    void SeekToLast() override {
        index_ = flist_->empty() ? 0 : flist_->size() - 1;
    }
    // void Next() override {
    //     assert(Valid());
    //     index_++;
    // }
    void Next() override {
        assert(Valid());
        index_++;
    }

    void Prev() override {
        assert(Valid());
        if (index_ == 0) {
            index_ = flist_->size();  // Marks as invalid
        } else {
            index_--;
        }
    }
    Slice key() const override {
        assert(Valid());
        return (*flist_)[index_]->largest.Encode();
    }
    Slice value() const override {
        assert(Valid());
        EncodeFixed64(value_buf_, (*flist_)[index_]->number);
        EncodeFixed64(value_buf_ + 8, (*flist_)[index_]->file_size);
        return Slice(value_buf_, sizeof(value_buf_));
    }
    Status status() const override { return Status::OK(); }

  private:
    const InternalKeyComparator icmp_;
    const std::vector<FileMetaData*>* const flist_;
    uint32_t index_;  // 通过index_定位sstable的位置

    // Backing store for value().  Holds the file number and size.
    mutable char value_buf_[16];  // file number + file size
};

/**
 * @brief 根据cache打开一个指向sstable文件的iter
 *
 * @param arg  table cache
 * @param options
 * @param file_value
 * @return Iterator*
 */
static Iterator* GetFileIterator(void* arg, const ReadOptions& options,
                                 const Slice& file_value) {
    TableCache* cache = reinterpret_cast<TableCache*>(arg);
    if (file_value.size() != 16) {
        return NewErrorIterator(
            Status::Corruption("FileReader invoked with unexpected value"));
    } else {
        return cache->NewIterator(options, DecodeFixed64(file_value.data()),
                                  DecodeFixed64(file_value.data() + 8));
    }
}

/**
 * @brief 返回一个two level iter，用于遍历一个level中的所有sstable
 * //! LevelFileNumIterator类似于table.cc中的index iter，file iterator相当于table.cc中的datablock iter
 * //! 通过Two Level Iterator将table和level files的串联迭代器进行复用
 * @param options
 * @param level
 * @return Iterator*
 */
Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
    //* 1. 创建一个指向level中所有sstable文件的迭代器，用于作为index iterator
    Iterator* index_iter =
        new LevelFileNumIterator(vset_->icmp_, &files_[level]);

    //* 2. 生成并返回一个two level iterator，用于遍历level 中的所有文件
    return NewTwoLevelIterator(index_iter, &GetFileIterator,
                               vset_->table_cache_, options);
}

/**
 * @brief 将当前Version中的指向每个Level的迭代器放入到iters中
 *
 * @param options
 * @param iters 输出迭代器列表
 */
void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
    // Merge all level zero files together since they may overlap
    //* 合并所有level 0的sstable文件，因为其可能有重叠
    //* 将level 0中的所有sstable file的iter放入iters中
    //* 对于level 0中的的一个文件用一个迭代器来遍历
    for (size_t i = 0; i < files_[0].size(); i++) {
        iters->push_back(vset_->table_cache_->NewIterator(
            options, files_[0][i]->number, files_[0][i]->file_size));
    }

    // For levels > 0, we can use a concatenating iterator that sequentially
    // walks through the non-overlapping files in the level, opening them
    // lazily.
    //* 对于level > 0的sstable文件是没有重叠overlap的部分的，通过串联的迭代器，用一个迭代器遍历一层的所有sstable
    //* 对于level > 0的每一层所有文件，只用一个two level iterator来进行遍历
    for (int level = 1; level < config::kNumLevels; level++) {
        if (!files_[level].empty()) {
            iters->push_back(NewConcatenatingIterator(options, level));
        }
    }
}

// Callback from TableCache::Get()
namespace {
enum SaverState {
    kNotFound,
    kFound,
    kDeleted,
    kCorrupt,
};
struct Saver {
    SaverState state;
    const Comparator* ucmp;
    Slice user_key;
    std::string* value;
};
}  // namespace
static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
    Saver* s = reinterpret_cast<Saver*>(arg);
    ParsedInternalKey parsed_key;
    if (!ParseInternalKey(ikey, &parsed_key)) {
        s->state = kCorrupt;
    } else {
        if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
            s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
            if (s->state == kFound) {
                s->value->assign(v.data(), v.size());
            }
        }
    }
}

static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
    return a->number > b->number;
}

/**
 * @brief 遍历所有文件,查询是否存在user key,如果存在则调用func进行处理,func如何返回false则直接return
 *
 * @param user_key
 * @param internal_key
 * @param arg
 * @param func   handle function return false代表结束处理
 */
void Version::ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                                 bool (*func)(void*, int, FileMetaData*)) {
    const Comparator* ucmp = vset_->icmp_.user_comparator();

    // Search level-0 in order from newest to oldest.
    // Create a temporary vector to store the file metadata.
    //* 1. 创建一个tmp list用于存放level 0的查找到的文件
    std::vector<FileMetaData*> tmp;
    // Reserve some space for the vector.
    tmp.reserve(files_[0].size());
    // Iterate through the files.

    //* 2. 遍历level 0的文件，将符合条件的文件添加到tmp list中
    for (uint32_t i = 0; i < files_[0].size(); i++) {
        // Get the file metadata.
        FileMetaData* f = files_[0][i];
        // Check if the file is within the user's range.
        if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
            ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
            // Add the file metadata to the temporary vector.
            tmp.push_back(
                f);  // 如果user key在文件最小key和最大key的范围内，则将其添加到临时向量中
        }
    }
    // If there are any files within the user's range.
    //* 3. 调用func对查询到的overlap file进行处理
    if (!tmp.empty()) {
        // Sort the files from newest to oldest.
        std::sort(
            tmp.begin(), tmp.end(),
            NewestFirst);  // 对tmp list进行排序, FileMetaData->number越大越新
        // Iterate through the files.
        for (uint32_t i = 0; i < tmp.size(); i++) {
            // Call the function with the file metadata.
            if (!(*func)(arg, 0, tmp[i])) {
                // Return if the function returns false.
                //* 如果func返回false，则直接返回
                return;
            }
        }
    }

    // Search other levels.
    //* 4. 搜索其他level的文件，是否有包含user key的文件
    for (int level = 1; level < config::kNumLevels; level++) {
        size_t num_files = files_[level].size();
        if (num_files == 0) continue;

        // Binary search to find earliest index whose largest key >= internal_key.
        uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
        if (index < num_files) {
            FileMetaData* f = files_[level][index];
            if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
                // All of "f" is past any data for user_key
            } else {
                if (!(*func)(arg, level, f)) {
                    return;
                }
            }
        }
    }
}

/**
 * @brief 调用ForEachoverlapping进行遍历,match函数则通过cache中的table进行internalget的table查找
 * match到了则退出并记录
 *
 * @param options
 * @param k
 * @param value
 * @param stats
 * @return Status
 */
Status Version::Get(const ReadOptions& options, const LookupKey& k,
                    std::string* value, GetStats* stats) {
    stats->seek_file = nullptr;
    stats->seek_file_level = -1;

    struct State {
        Saver saver;
        GetStats* stats;
        const ReadOptions* options;
        Slice ikey;
        FileMetaData* last_file_read;
        int last_file_read_level;

        VersionSet* vset;
        Status s;
        bool found;

        static bool Match(void* arg, int level, FileMetaData* f) {
            State* state = reinterpret_cast<State*>(arg);

            if (state->stats->seek_file == nullptr &&
                state->last_file_read != nullptr) {
                // We have had more than one seek for this read.  Charge the 1st file.
                //* 一次read可能match到多次,记录第一个match到的文件到seek_file中
                state->stats->seek_file = state->last_file_read;
                state->stats->seek_file_level = state->last_file_read_level;
            }

            state->last_file_read = f;
            state->last_file_read_level = level;

            state->s = state->vset->table_cache_->Get(
                *state->options, f->number, f->file_size, state->ikey,
                &state->saver, SaveValue);
            if (!state->s.ok()) {
                state->found = true;
                return false;
            }
            switch (state->saver.state) {
                case kNotFound:
                    return true;  // Keep searching in other files
                                  // return true说明还需要继续match
                case kFound:
                    state->found = true;
                    return false;
                case kDeleted:
                    return false;
                case kCorrupt:
                    state->s = Status::Corruption("corrupted key for ",
                                                  state->saver.user_key);
                    state->found = true;
                    return false;
            }

            // Not reached. Added to avoid false compilation warnings of
            // "control reaches end of non-void function".
            return false;
        }
    };

    State state;
    state.found = false;
    state.stats = stats;
    state.last_file_read = nullptr;
    state.last_file_read_level = -1;

    state.options = &options;
    state.ikey = k.internal_key();
    state.vset = vset_;

    state.saver.state = kNotFound;
    state.saver.ucmp = vset_->icmp_.user_comparator();
    state.saver.user_key = k.user_key();
    state.saver.value = value;

    //* 针对每个overlap了的key，进行match
    ForEachOverlapping(state.saver.user_key, state.ikey, &state, &State::Match);

    // state.found时: 真的found到了/iter出现错误status报错
    return state.found ? state.s : Status::NotFound(Slice());
}

bool Version::UpdateStats(const GetStats& stats) {
    FileMetaData* f = stats.seek_file;
    if (f != nullptr) {
        f->allowed_seeks--;
        if (f->allowed_seeks <= 0 && file_to_compact_ == nullptr) {
            file_to_compact_ = f;
            file_to_compact_level_ = stats.seek_file_level;
            return true;
        }
    }
    return false;
}

bool Version::RecordReadSample(Slice internal_key) {
    //* step1. 从interanl key中进行解析得到type sequence user_key
    ParsedInternalKey ikey;
    if (!ParseInternalKey(internal_key, &ikey)) {
        return false;
    }

    //* step2. 定义新的State结构体
    struct State {
        GetStats stats;  // Holds first matching file
        int matches;

        static bool Match(void* arg, int level, FileMetaData* f) {
            State* state = reinterpret_cast<State*>(arg);
            state->matches++;
            if (state->matches == 1) {
                // Remember first match.
                // 记住第一次匹配到的文件
                state->stats.seek_file = f;
                state->stats.seek_file_level = level;
            }
            // We can stop iterating once we have a second match.
            return state->matches < 2;
        }
    };

    State state;
    state.matches = 0;

    //* 遍历所有包含ikey的sstable文件，进行match，判断ikey是否重复出现，如果是则直接return
    ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

    // Must have at least two matches since we want to merge across
    // files. But what if we have a single file that contains many
    // overwrites and deletions?  Should we have another mechanism for
    // finding such files?
    //* 多次match到，说明userkey在多个文件中出现，需要进行merge
    if (state.matches >= 2) {
        // 1MB cost is about 1 seek (see comment in Builder::Apply).
        return UpdateStats(state.stats);
    }
    return false;
}

void Version::Ref() { ++refs_; }

void Version::Unref() {
    assert(this != &vset_->dummy_versions_);
    assert(refs_ >= 1);
    --refs_;
    if (refs_ == 0) {
        delete this;
    }
}

bool Version::OverlapInLevel(int level, const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
    return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                                 smallest_user_key, largest_user_key);
}

/**
 * @brief 给定一个memtable的key值范围，选择一个合适的level作为memtable的输出
 *
 * @param smallest_user_key
 * @param largest_user_key
 * @return int
 */
int Version::PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                        const Slice& largest_user_key) {
    int level = 0;

    //* 如果没有overlap，则不需要放到第0层，向下寻找合适的层数
    if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
        // Push to next level if there is no overlap in next level,
        // and the #bytes overlapping in the level after that are limited.
        InternalKey start(smallest_user_key, kMaxSequenceNumber,
                          kValueTypeForSeek);
        InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
        std::vector<FileMetaData*> overlaps;
        while (level < config::kMaxMemCompactLevel) {
            if (OverlapInLevel(level + 1, &smallest_user_key,
                               &largest_user_key)) {
                // 如果下一层overlap了，则直接跳出，还是在当前层输出
                break;
            }
            if (level + 2 < config::kNumLevels) {
                // Check that file does not overlap too many grandparent bytes.
                // 如果下下层overlap了当前key的文件太大，则直接跳出，还是在当前层输出
                GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
                const int64_t sum = TotalFileSize(overlaps);
                if (sum > MaxGrandParentOverlapBytes(vset_->options_)) {
                    break;
                }
            }
            level++;
        }
    }

    //* 存在overlap只能放到level 0，等待合并
    return level;
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
/**
 * @brief 将level中所有包含[begin, end]的文件放入到inputs中
 *
 * @param level
 * @param begin
 * @param end
 * @param inputs
 */
void Version::GetOverlappingInputs(int level, const InternalKey* begin,
                                   const InternalKey* end,
                                   std::vector<FileMetaData*>* inputs) {
    assert(level >= 0);
    assert(level < config::kNumLevels);
    inputs->clear();
    Slice user_begin, user_end;
    if (begin != nullptr) {
        user_begin = begin->user_key();
    }
    if (end != nullptr) {
        user_end = end->user_key();
    }
    auto cmp = vset_->icmp_.user_comparator();
    const Comparator* user_cmp = vset_->icmp_.user_comparator();
    for (size_t i = 0; i < files_[level].size();) {
        FileMetaData* f = files_[level][i++];
        const Slice file_start = f->smallest.user_key();
        const Slice file_limit = f->largest.user_key();
        if (begin != nullptr && user_cmp->Compare(file_limit, user_begin) < 0) {
            // "f" is completely before specified range; skip it
        } else if (end != nullptr &&
                   user_cmp->Compare(file_start, user_end) > 0) {
            // "f" is completely after specified range; skip it
        } else {
            //* files_begin -- files_end 包含了给定的user_begin, user_end的其中一部分
            inputs->push_back(f);
            if (level == 0) {
                // Level-0 files may overlap each other.  So check if the newly
                // added file has expanded the range.  If so, restart search.
                //* Level 0的文件可能有重叠。检查新文件是否扩展了之前的范围，如果是，则扩展userkey范围并从头开始搜索
                //* Level-0 file1: [30, 100], file2: [70, 200]
                //* user range: [80, 150]
                //* 1. file1和user range有重叠，将其加入inputs中
                //* 2. file2和user range有重叠，将其加入inputs中
                //* 3. file2的file_start小于user begin，此时只需要一个file2就能将user range完全覆盖，因此将user begin置为70，并清空inputs从头开始搜索
                //* 4. 从头开始搜索，file1不满足条件，但是file2能加入inputs中
                if (begin != nullptr &&
                    user_cmp->Compare(file_start, user_begin) < 0) {
                    user_begin = file_start;
                    inputs->clear();
                    i = 0;
                } else if (end != nullptr &&
                           user_cmp->Compare(file_limit, user_end) > 0) {
                    user_end = file_limit;
                    inputs->clear();
                    i = 0;
                }
            }
        }
    }
}

std::string Version::DebugString() const {
    std::string r;
    for (int level = 0; level < config::kNumLevels; level++) {
        // E.g.,
        //   --- level 1 ---
        //   17:123['a' .. 'd']
        //   20:43['e' .. 'g']
        r.append("--- level ");
        AppendNumberTo(&r, level);
        r.append(" ---\n");
        const std::vector<FileMetaData*>& files = files_[level];
        for (size_t i = 0; i < files.size(); i++) {
            r.push_back(' ');
            AppendNumberTo(&r, files[i]->number);
            r.push_back(':');
            AppendNumberTo(&r, files[i]->file_size);
            r.append("[");
            r.append(files[i]->smallest.DebugString());
            r.append(" .. ");
            r.append(files[i]->largest.DebugString());
            r.append("]\n");
        }
    }
    return r;
}

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class VersionSet::Builder {
  private:
    // Helper to sort by v->files_[file_number].smallest
    struct BySmallestKey {
        const InternalKeyComparator* internal_comparator;

        bool operator()(FileMetaData* f1, FileMetaData* f2) const {
            int r = internal_comparator->Compare(f1->smallest, f2->smallest);
            if (r != 0) {
                return (r < 0);
            } else {
                // Break ties by file number
                return (f1->number < f2->number);
            }
        }
    };

    typedef std::set<FileMetaData*, BySmallestKey> FileSet;
    struct LevelState {
        std::set<uint64_t> deleted_files;
        FileSet* added_files;
    };

    VersionSet* vset_;
    Version* base_;
    LevelState levels_[config::kNumLevels];

  public:
    // Initialize a builder with the files from *base and other info from *vset
    Builder(VersionSet* vset, Version* base) : vset_(vset), base_(base) {
        base_->Ref();
        BySmallestKey cmp;
        cmp.internal_comparator = &vset_->icmp_;
        for (int level = 0; level < config::kNumLevels; level++) {
            levels_[level].added_files = new FileSet(cmp);
        }
    }

    ~Builder() {
        for (int level = 0; level < config::kNumLevels; level++) {
            const FileSet* added = levels_[level].added_files;
            std::vector<FileMetaData*> to_unref;
            to_unref.reserve(added->size());
            for (FileSet::const_iterator it = added->begin();
                 it != added->end(); ++it) {
                to_unref.push_back(*it);
            }
            delete added;
            for (uint32_t i = 0; i < to_unref.size(); i++) {
                FileMetaData* f = to_unref[i];
                f->refs--;
                if (f->refs <= 0) {
                    delete f;
                }
            }
        }
        base_->Unref();
    }

    // Apply all of the edits in *edit to the current state.
    void Apply(const VersionEdit* edit) {
        // Update compaction pointers
        for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
            const int level = edit->compact_pointers_[i].first;
            vset_->compact_pointer_[level] =
                edit->compact_pointers_[i].second.Encode().ToString();
        }

        // Delete files
        for (const auto& deleted_file_set_kvp : edit->deleted_files_) {
            const int level = deleted_file_set_kvp.first;
            const uint64_t number = deleted_file_set_kvp.second;
            levels_[level].deleted_files.insert(number);
        }

        // Add new files
        for (size_t i = 0; i < edit->new_files_.size(); i++) {
            const int level = edit->new_files_[i].first;
            FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
            f->refs = 1;

            // We arrange to automatically compact this file after
            // a certain number of seeks.  Let's assume:
            //   (1) One seek costs 10ms
            //   (2) Writing or reading 1MB costs 10ms (100MB/s)
            //   (3) A compaction of 1MB does 25MB of IO:
            //         1MB read from this level
            //         10-12MB read from next level (boundaries may be misaligned)
            //         10-12MB written to next level
            // This implies that 25 seeks cost the same as the compaction
            // of 1MB of data.  I.e., one seek costs approximately the
            // same as the compaction of 40KB of data.  We are a little
            // conservative and allow approximately one seek for every 16KB
            // of data before triggering a compaction.
            f->allowed_seeks = static_cast<int>((f->file_size / 16384U));
            if (f->allowed_seeks < 100) f->allowed_seeks = 100;

            levels_[level].deleted_files.erase(f->number);
            levels_[level].added_files->insert(f);
        }
    }

    // Save the current state in *v.
    void SaveTo(Version* v) {
        BySmallestKey cmp;
        cmp.internal_comparator = &vset_->icmp_;
        for (int level = 0; level < config::kNumLevels; level++) {
            // Merge the set of added files with the set of pre-existing files.
            // Drop any deleted files.  Store the result in *v.
            const std::vector<FileMetaData*>& base_files = base_->files_[level];
            std::vector<FileMetaData*>::const_iterator base_iter =
                base_files.begin();
            std::vector<FileMetaData*>::const_iterator base_end =
                base_files.end();
            const FileSet* added_files = levels_[level].added_files;
            v->files_[level].reserve(base_files.size() + added_files->size());
            for (const auto& added_file : *added_files) {
                // Add all smaller files listed in base_
                for (std::vector<FileMetaData*>::const_iterator bpos =
                         std::upper_bound(base_iter, base_end, added_file, cmp);
                     base_iter != bpos; ++base_iter) {
                    MaybeAddFile(v, level, *base_iter);
                }

                MaybeAddFile(v, level, added_file);
            }

            // Add remaining base files
            for (; base_iter != base_end; ++base_iter) {
                MaybeAddFile(v, level, *base_iter);
            }

#ifndef NDEBUG
            // Make sure there is no overlap in levels > 0
            if (level > 0) {
                for (uint32_t i = 1; i < v->files_[level].size(); i++) {
                    const InternalKey& prev_end =
                        v->files_[level][i - 1]->largest;
                    const InternalKey& this_begin =
                        v->files_[level][i]->smallest;
                    if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
                        std::fprintf(
                            stderr,
                            "overlapping ranges in same level %s vs. %s\n",
                            prev_end.DebugString().c_str(),
                            this_begin.DebugString().c_str());
                        std::abort();
                    }
                }
            }
#endif
        }
    }

    void MaybeAddFile(Version* v, int level, FileMetaData* f) {
        if (levels_[level].deleted_files.count(f->number) > 0) {
            // File is deleted: do nothing
        } else {
            std::vector<FileMetaData*>* files = &v->files_[level];
            if (level > 0 && !files->empty()) {
                // Must not overlap
                assert(vset_->icmp_.Compare(
                           (*files)[files->size() - 1]->largest, f->smallest) <
                       0);
            }
            f->refs++;
            files->push_back(f);
        }
    }
};

VersionSet::VersionSet(const std::string& dbname, const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_file_(nullptr),
      descriptor_log_(nullptr),
      dummy_versions_(this),
      current_(nullptr) {
    AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
    current_->Unref();
    assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
    delete descriptor_log_;
    delete descriptor_file_;
}

void VersionSet::AppendVersion(Version* v) {
    // Make "v" current
    assert(v->refs_ == 0);
    assert(v != current_);
    if (current_ != nullptr) {
        current_->Unref();
    }
    current_ = v;
    v->Ref();

    // Append to linked list
    v->prev_ = dummy_versions_.prev_;
    v->next_ = &dummy_versions_;
    v->prev_->next_ = v;
    v->next_->prev_ = v;
}

Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {
    if (edit->has_log_number_) {
        assert(edit->log_number_ >= log_number_);
        assert(edit->log_number_ < next_file_number_);
    } else {
        edit->SetLogNumber(log_number_);
    }

    if (!edit->has_prev_log_number_) {
        edit->SetPrevLogNumber(prev_log_number_);
    }

    edit->SetNextFile(next_file_number_);
    edit->SetLastSequence(last_sequence_);

    Version* v = new Version(this);
    {
        Builder builder(this, current_);
        builder.Apply(edit);
        builder.SaveTo(v);
    }
    Finalize(v);

    // Initialize new descriptor log file if necessary by creating
    // a temporary file that contains a snapshot of the current version.
    std::string new_manifest_file;
    Status s;
    if (descriptor_log_ == nullptr) {
        // No reason to unlock *mu here since we only hit this path in the
        // first call to LogAndApply (when opening the database).
        assert(descriptor_file_ == nullptr);
        new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
        s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
        if (s.ok()) {
            descriptor_log_ = new log::Writer(descriptor_file_);
            s = WriteSnapshot(descriptor_log_);
        }
    }

    // Unlock during expensive MANIFEST log write
    {
        mu->Unlock();

        // Write new record to MANIFEST log
        if (s.ok()) {
            std::string record;
            edit->EncodeTo(&record);
            s = descriptor_log_->AddRecord(record);
            if (s.ok()) {
                s = descriptor_file_->Sync();
            }
            if (!s.ok()) {
                Log(options_->info_log, "MANIFEST write: %s\n",
                    s.ToString().c_str());
            }
        }

        // If we just created a new descriptor file, install it by writing a
        // new CURRENT file that points to it.
        if (s.ok() && !new_manifest_file.empty()) {
            s = SetCurrentFile(env_, dbname_, manifest_file_number_);
        }

        mu->Lock();
    }

    // Install the new version
    if (s.ok()) {
        AppendVersion(v);
        log_number_ = edit->log_number_;
        prev_log_number_ = edit->prev_log_number_;
    } else {
        delete v;
        if (!new_manifest_file.empty()) {
            delete descriptor_log_;
            delete descriptor_file_;
            descriptor_log_ = nullptr;
            descriptor_file_ = nullptr;
            env_->RemoveFile(new_manifest_file);
        }
    }

    return s;
}

Status VersionSet::Recover(bool* save_manifest) {
    struct LogReporter : public log::Reader::Reporter {
        Status* status;
        void Corruption(size_t bytes, const Status& s) override {
            if (this->status->ok()) *this->status = s;
        }
    };

    // Read "CURRENT" file, which contains a pointer to the current manifest file
    std::string current;
    Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
    if (!s.ok()) {
        return s;
    }
    if (current.empty() || current[current.size() - 1] != '\n') {
        return Status::Corruption("CURRENT file does not end with newline");
    }
    current.resize(current.size() - 1);

    std::string dscname = dbname_ + "/" + current;
    SequentialFile* file;
    s = env_->NewSequentialFile(dscname, &file);
    if (!s.ok()) {
        if (s.IsNotFound()) {
            return Status::Corruption("CURRENT points to a non-existent file",
                                      s.ToString());
        }
        return s;
    }

    bool have_log_number = false;
    bool have_prev_log_number = false;
    bool have_next_file = false;
    bool have_last_sequence = false;
    uint64_t next_file = 0;
    uint64_t last_sequence = 0;
    uint64_t log_number = 0;
    uint64_t prev_log_number = 0;
    Builder builder(this, current_);
    int read_records = 0;

    {
        LogReporter reporter;
        reporter.status = &s;
        log::Reader reader(file, &reporter, true /*checksum*/,
                           0 /*initial_offset*/);
        Slice record;
        std::string scratch;
        while (reader.ReadRecord(&record, &scratch) && s.ok()) {
            ++read_records;
            VersionEdit edit;
            s = edit.DecodeFrom(record);
            if (s.ok()) {
                if (edit.has_comparator_ &&
                    edit.comparator_ != icmp_.user_comparator()->Name()) {
                    s = Status::InvalidArgument(
                        edit.comparator_ +
                            " does not match existing comparator ",
                        icmp_.user_comparator()->Name());
                }
            }

            if (s.ok()) {
                builder.Apply(&edit);
            }

            if (edit.has_log_number_) {
                log_number = edit.log_number_;
                have_log_number = true;
            }

            if (edit.has_prev_log_number_) {
                prev_log_number = edit.prev_log_number_;
                have_prev_log_number = true;
            }

            if (edit.has_next_file_number_) {
                next_file = edit.next_file_number_;
                have_next_file = true;
            }

            if (edit.has_last_sequence_) {
                last_sequence = edit.last_sequence_;
                have_last_sequence = true;
            }
        }
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
        if (!have_next_file) {
            s = Status::Corruption("no meta-nextfile entry in descriptor");
        } else if (!have_log_number) {
            s = Status::Corruption("no meta-lognumber entry in descriptor");
        } else if (!have_last_sequence) {
            s = Status::Corruption(
                "no last-sequence-number entry in descriptor");
        }

        if (!have_prev_log_number) {
            prev_log_number = 0;
        }

        MarkFileNumberUsed(prev_log_number);
        MarkFileNumberUsed(log_number);
    }

    if (s.ok()) {
        Version* v = new Version(this);
        builder.SaveTo(v);
        // Install recovered version
        Finalize(v);
        AppendVersion(v);
        manifest_file_number_ = next_file;
        next_file_number_ = next_file + 1;
        last_sequence_ = last_sequence;
        log_number_ = log_number;
        prev_log_number_ = prev_log_number;

        // See if we can reuse the existing MANIFEST file.
        if (ReuseManifest(dscname, current)) {
            // No need to save new manifest
        } else {
            *save_manifest = true;
        }
    } else {
        std::string error = s.ToString();
        Log(options_->info_log,
            "Error recovering version set with %d records: %s", read_records,
            error.c_str());
    }

    return s;
}

bool VersionSet::ReuseManifest(const std::string& dscname,
                               const std::string& dscbase) {
    if (!options_->reuse_logs) {
        return false;
    }
    FileType manifest_type;
    uint64_t manifest_number;
    uint64_t manifest_size;
    if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
        manifest_type != kDescriptorFile ||
        !env_->GetFileSize(dscname, &manifest_size).ok() ||
        // Make new compacted MANIFEST if old one is too big
        manifest_size >= TargetFileSize(options_)) {
        return false;
    }

    assert(descriptor_file_ == nullptr);
    assert(descriptor_log_ == nullptr);
    Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
    if (!r.ok()) {
        Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
        assert(descriptor_file_ == nullptr);
        return false;
    }

    Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
    descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
    manifest_file_number_ = manifest_number;
    return true;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
    if (next_file_number_ <= number) {
        next_file_number_ = number + 1;
    }
}

void VersionSet::Finalize(Version* v) {
    // Precomputed best level for next compaction
    int best_level = -1;
    double best_score = -1;

    for (int level = 0; level < config::kNumLevels - 1; level++) {
        double score;
        if (level == 0) {
            // We treat level-0 specially by bounding the number of files
            // instead of number of bytes for two reasons:
            //
            // (1) With larger write-buffer sizes, it is nice not to do too
            // many level-0 compactions.
            //
            // (2) The files in level-0 are merged on every read and
            // therefore we wish to avoid too many files when the individual
            // file size is small (perhaps because of a small write-buffer
            // setting, or very high compression ratios, or lots of
            // overwrites/deletions).
            score = v->files_[level].size() /
                    static_cast<double>(config::kL0_CompactionTrigger);
        } else {
            // Compute the ratio of current size to size limit.
            const uint64_t level_bytes = TotalFileSize(v->files_[level]);
            score = static_cast<double>(level_bytes) /
                    MaxBytesForLevel(options_, level);
        }

        if (score > best_score) {
            best_level = level;
            best_score = score;
        }
    }

    v->compaction_level_ = best_level;
    v->compaction_score_ = best_score;
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
    // TODO: Break up into multiple records to reduce memory usage on recovery?

    // Save metadata
    VersionEdit edit;
    edit.SetComparatorName(icmp_.user_comparator()->Name());

    // Save compaction pointers
    for (int level = 0; level < config::kNumLevels; level++) {
        if (!compact_pointer_[level].empty()) {
            InternalKey key;
            key.DecodeFrom(compact_pointer_[level]);
            edit.SetCompactPointer(level, key);
        }
    }

    // Save files
    for (int level = 0; level < config::kNumLevels; level++) {
        const std::vector<FileMetaData*>& files = current_->files_[level];
        for (size_t i = 0; i < files.size(); i++) {
            const FileMetaData* f = files[i];
            edit.AddFile(level, f->number, f->file_size, f->smallest,
                         f->largest);
        }
    }

    std::string record;
    edit.EncodeTo(&record);
    return log->AddRecord(record);
}

int VersionSet::NumLevelFiles(int level) const {
    assert(level >= 0);
    assert(level < config::kNumLevels);
    return current_->files_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
    // Update code if kNumLevels changes
    static_assert(config::kNumLevels == 7, "");
    std::snprintf(
        scratch->buffer, sizeof(scratch->buffer),
        "files[ %d %d %d %d %d %d %d ]", int(current_->files_[0].size()),
        int(current_->files_[1].size()), int(current_->files_[2].size()),
        int(current_->files_[3].size()), int(current_->files_[4].size()),
        int(current_->files_[5].size()), int(current_->files_[6].size()));
    return scratch->buffer;
}

uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
    uint64_t result = 0;
    for (int level = 0; level < config::kNumLevels; level++) {
        const std::vector<FileMetaData*>& files = v->files_[level];
        for (size_t i = 0; i < files.size(); i++) {
            if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
                // Entire file is before "ikey", so just add the file size
                result += files[i]->file_size;
            } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
                // Entire file is after "ikey", so ignore
                if (level > 0) {
                    // Files other than level 0 are sorted by meta->smallest, so
                    // no further files in this level will contain data for
                    // "ikey".
                    break;
                }
            } else {
                // "ikey" falls in the range for this table.  Add the
                // approximate offset of "ikey" within the table.
                Table* tableptr;
                Iterator* iter =
                    table_cache_->NewIterator(ReadOptions(), files[i]->number,
                                              files[i]->file_size, &tableptr);
                if (tableptr != nullptr) {
                    result += tableptr->ApproximateOffsetOf(ikey.Encode());
                }
                delete iter;
            }
        }
    }
    return result;
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
    for (Version* v = dummy_versions_.next_; v != &dummy_versions_;
         v = v->next_) {
        for (int level = 0; level < config::kNumLevels; level++) {
            const std::vector<FileMetaData*>& files = v->files_[level];
            for (size_t i = 0; i < files.size(); i++) {
                live->insert(files[i]->number);
            }
        }
    }
}

int64_t VersionSet::NumLevelBytes(int level) const {
    assert(level >= 0);
    assert(level < config::kNumLevels);
    return TotalFileSize(current_->files_[level]);
}

int64_t VersionSet::MaxNextLevelOverlappingBytes() {
    int64_t result = 0;
    std::vector<FileMetaData*> overlaps;
    for (int level = 1; level < config::kNumLevels - 1; level++) {
        for (size_t i = 0; i < current_->files_[level].size(); i++) {
            const FileMetaData* f = current_->files_[level][i];
            current_->GetOverlappingInputs(level + 1, &f->smallest, &f->largest,
                                           &overlaps);
            const int64_t sum = TotalFileSize(overlaps);
            if (sum > result) {
                result = sum;
            }
        }
    }
    return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest, InternalKey* largest) {
    assert(!inputs.empty());
    smallest->Clear();
    largest->Clear();
    for (size_t i = 0; i < inputs.size(); i++) {
        FileMetaData* f = inputs[i];
        if (i == 0) {
            *smallest = f->smallest;
            *largest = f->largest;
        } else {
            if (icmp_.Compare(f->smallest, *smallest) < 0) {
                *smallest = f->smallest;
            }
            if (icmp_.Compare(f->largest, *largest) > 0) {
                *largest = f->largest;
            }
        }
    }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest, InternalKey* largest) {
    std::vector<FileMetaData*> all = inputs1;
    all.insert(all.end(), inputs2.begin(), inputs2.end());
    GetRange(all, smallest, largest);
}

Iterator* VersionSet::MakeInputIterator(Compaction* c) {
    ReadOptions options;
    options.verify_checksums = options_->paranoid_checks;
    options.fill_cache = false;

    // Level-0 files have to be merged together.  For other levels,
    // we will make a concatenating iterator per level.
    // TODO(opt): use concatenating iterator for level-0 if there is no overlap
    const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
    Iterator** list = new Iterator*[space];
    int num = 0;
    for (int which = 0; which < 2; which++) {
        if (!c->inputs_[which].empty()) {
            if (c->level() + which == 0) {
                const std::vector<FileMetaData*>& files = c->inputs_[which];
                for (size_t i = 0; i < files.size(); i++) {
                    list[num++] = table_cache_->NewIterator(
                        options, files[i]->number, files[i]->file_size);
                }
            } else {
                // Create concatenating iterator for the files from this level
                list[num++] = NewTwoLevelIterator(
                    new Version::LevelFileNumIterator(icmp_,
                                                      &c->inputs_[which]),
                    &GetFileIterator, table_cache_, options);
            }
        }
    }
    assert(num <= space);
    Iterator* result = NewMergingIterator(&icmp_, list, num);
    delete[] list;
    return result;
}

Compaction* VersionSet::PickCompaction() {
    Compaction* c;
    int level;

    // We prefer compactions triggered by too much data in a level over
    // the compactions triggered by seeks.
    const bool size_compaction = (current_->compaction_score_ >= 1);
    const bool seek_compaction = (current_->file_to_compact_ != nullptr);
    if (size_compaction) {
        level = current_->compaction_level_;
        assert(level >= 0);
        assert(level + 1 < config::kNumLevels);
        c = new Compaction(options_, level);

        // Pick the first file that comes after compact_pointer_[level]
        for (size_t i = 0; i < current_->files_[level].size(); i++) {
            FileMetaData* f = current_->files_[level][i];
            if (compact_pointer_[level].empty() ||
                icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) >
                    0) {
                c->inputs_[0].push_back(f);
                break;
            }
        }
        if (c->inputs_[0].empty()) {
            // Wrap-around to the beginning of the key space
            c->inputs_[0].push_back(current_->files_[level][0]);
        }
    } else if (seek_compaction) {
        level = current_->file_to_compact_level_;
        c = new Compaction(options_, level);
        c->inputs_[0].push_back(current_->file_to_compact_);
    } else {
        return nullptr;
    }

    c->input_version_ = current_;
    c->input_version_->Ref();

    // Files in level 0 may overlap each other, so pick up all overlapping ones
    if (level == 0) {
        InternalKey smallest, largest;
        GetRange(c->inputs_[0], &smallest, &largest);
        // Note that the next call will discard the file we placed in
        // c->inputs_[0] earlier and replace it with an overlapping set
        // which will include the picked file.
        current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
        assert(!c->inputs_[0].empty());
    }

    SetupOtherInputs(c);

    return c;
}

// Finds the largest key in a vector of files. Returns true if files is not
// empty.
bool FindLargestKey(const InternalKeyComparator& icmp,
                    const std::vector<FileMetaData*>& files,
                    InternalKey* largest_key) {
    if (files.empty()) {
        return false;
    }
    *largest_key = files[0]->largest;
    for (size_t i = 1; i < files.size(); ++i) {
        FileMetaData* f = files[i];
        if (icmp.Compare(f->largest, *largest_key) > 0) {
            *largest_key = f->largest;
        }
    }
    return true;
}

// Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
// user_key(l2) = user_key(u1)
FileMetaData* FindSmallestBoundaryFile(
    const InternalKeyComparator& icmp,
    const std::vector<FileMetaData*>& level_files,
    const InternalKey& largest_key) {
    const Comparator* user_cmp = icmp.user_comparator();
    FileMetaData* smallest_boundary_file = nullptr;
    for (size_t i = 0; i < level_files.size(); ++i) {
        FileMetaData* f = level_files[i];
        if (icmp.Compare(f->smallest, largest_key) > 0 &&
            user_cmp->Compare(f->smallest.user_key(), largest_key.user_key()) ==
                0) {
            if (smallest_boundary_file == nullptr ||
                icmp.Compare(f->smallest, smallest_boundary_file->smallest) <
                    0) {
                smallest_boundary_file = f;
            }
        }
    }
    return smallest_boundary_file;
}

// Extracts the largest file b1 from |compaction_files| and then searches for a
// b2 in |level_files| for which user_key(u1) = user_key(l2). If it finds such a
// file b2 (known as a boundary file) it adds it to |compaction_files| and then
// searches again using this new upper bound.
//
// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it searches
// level by level for records matching the supplied user key.
//
// parameters:
//   in     level_files:      List of files to search for boundary files.
//   in/out compaction_files: List of files to extend by adding boundary files.
void AddBoundaryInputs(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>& level_files,
                       std::vector<FileMetaData*>* compaction_files) {
    InternalKey largest_key;

    // Quick return if compaction_files is empty.
    if (!FindLargestKey(icmp, *compaction_files, &largest_key)) {
        return;
    }

    bool continue_searching = true;
    while (continue_searching) {
        FileMetaData* smallest_boundary_file =
            FindSmallestBoundaryFile(icmp, level_files, largest_key);

        // If a boundary file was found advance largest_key, otherwise we're done.
        if (smallest_boundary_file != NULL) {
            compaction_files->push_back(smallest_boundary_file);
            largest_key = smallest_boundary_file->largest;
        } else {
            continue_searching = false;
        }
    }
}

void VersionSet::SetupOtherInputs(Compaction* c) {
    const int level = c->level();

    InternalKey smallest, largest;

    AddBoundaryInputs(icmp_, current_->files_[level], &c->inputs_[0]);
    GetRange(c->inputs_[0], &smallest, &largest);

    current_->GetOverlappingInputs(level + 1, &smallest, &largest,
                                   &c->inputs_[1]);
    AddBoundaryInputs(icmp_, current_->files_[level + 1], &c->inputs_[1]);

    // Get entire range covered by compaction
    InternalKey all_start, all_limit;
    GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

    // See if we can grow the number of inputs in "level" without
    // changing the number of "level+1" files we pick up.
    if (!c->inputs_[1].empty()) {
        std::vector<FileMetaData*> expanded0;
        current_->GetOverlappingInputs(level, &all_start, &all_limit,
                                       &expanded0);
        AddBoundaryInputs(icmp_, current_->files_[level], &expanded0);
        const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
        const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
        const int64_t expanded0_size = TotalFileSize(expanded0);
        if (expanded0.size() > c->inputs_[0].size() &&
            inputs1_size + expanded0_size <
                ExpandedCompactionByteSizeLimit(options_)) {
            InternalKey new_start, new_limit;
            GetRange(expanded0, &new_start, &new_limit);
            std::vector<FileMetaData*> expanded1;
            current_->GetOverlappingInputs(level + 1, &new_start, &new_limit,
                                           &expanded1);
            AddBoundaryInputs(icmp_, current_->files_[level + 1], &expanded1);
            if (expanded1.size() == c->inputs_[1].size()) {
                Log(options_->info_log,
                    "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld "
                    "bytes)\n",
                    level, int(c->inputs_[0].size()), int(c->inputs_[1].size()),
                    long(inputs0_size), long(inputs1_size),
                    int(expanded0.size()), int(expanded1.size()),
                    long(expanded0_size), long(inputs1_size));
                smallest = new_start;
                largest = new_limit;
                c->inputs_[0] = expanded0;
                c->inputs_[1] = expanded1;
                GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
            }
        }
    }

    // Compute the set of grandparent files that overlap this compaction
    // (parent == level+1; grandparent == level+2)
    if (level + 2 < config::kNumLevels) {
        current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                       &c->grandparents_);
    }

    // Update the place where we will do the next compaction for this level.
    // We update this immediately instead of waiting for the VersionEdit
    // to be applied so that if the compaction fails, we will try a different
    // key range next time.
    compact_pointer_[level] = largest.Encode().ToString();
    c->edit_.SetCompactPointer(level, largest);
}

Compaction* VersionSet::CompactRange(int level, const InternalKey* begin,
                                     const InternalKey* end) {
    std::vector<FileMetaData*> inputs;
    current_->GetOverlappingInputs(level, begin, end, &inputs);
    if (inputs.empty()) {
        return nullptr;
    }

    // Avoid compacting too much in one shot in case the range is large.
    // But we cannot do this for level-0 since level-0 files can overlap
    // and we must not pick one file and drop another older file if the
    // two files overlap.
    if (level > 0) {
        const uint64_t limit = MaxFileSizeForLevel(options_, level);
        uint64_t total = 0;
        for (size_t i = 0; i < inputs.size(); i++) {
            uint64_t s = inputs[i]->file_size;
            total += s;
            if (total >= limit) {
                inputs.resize(i + 1);
                break;
            }
        }
    }

    Compaction* c = new Compaction(options_, level);
    c->input_version_ = current_;
    c->input_version_->Ref();
    c->inputs_[0] = inputs;
    SetupOtherInputs(c);
    return c;
}

Compaction::Compaction(const Options* options, int level)
    : level_(level),
      max_output_file_size_(MaxFileSizeForLevel(options, level)),
      input_version_(nullptr),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0) {
    for (int i = 0; i < config::kNumLevels; i++) {
        level_ptrs_[i] = 0;
    }
}

Compaction::~Compaction() {
    if (input_version_ != nullptr) {
        input_version_->Unref();
    }
}

bool Compaction::IsTrivialMove() const {
    const VersionSet* vset = input_version_->vset_;
    // Avoid a move if there is lots of overlapping grandparent data.
    // Otherwise, the move could create a parent file that will require
    // a very expensive merge later on.
    return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
            TotalFileSize(grandparents_) <=
                MaxGrandParentOverlapBytes(vset->options_));
}

void Compaction::AddInputDeletions(VersionEdit* edit) {
    for (int which = 0; which < 2; which++) {
        for (size_t i = 0; i < inputs_[which].size(); i++) {
            edit->RemoveFile(level_ + which, inputs_[which][i]->number);
        }
    }
}

bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
    // Maybe use binary search to find right entry instead of linear search?
    const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
    for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
        const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
        while (level_ptrs_[lvl] < files.size()) {
            FileMetaData* f = files[level_ptrs_[lvl]];
            if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
                // We've advanced far enough
                if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
                    // Key falls in this file's range, so definitely not base level
                    return false;
                }
                break;
            }
            level_ptrs_[lvl]++;
        }
    }
    return true;
}

bool Compaction::ShouldStopBefore(const Slice& internal_key) {
    const VersionSet* vset = input_version_->vset_;
    // Scan to find earliest grandparent file that contains key.
    const InternalKeyComparator* icmp = &vset->icmp_;
    while (grandparent_index_ < grandparents_.size() &&
           icmp->Compare(internal_key,
                         grandparents_[grandparent_index_]->largest.Encode()) >
               0) {
        if (seen_key_) {
            overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
        }
        grandparent_index_++;
    }
    seen_key_ = true;

    if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) {
        // Too much overlap for current output; start new output
        overlapped_bytes_ = 0;
        return true;
    } else {
        return false;
    }
}

void Compaction::ReleaseInputs() {
    if (input_version_ != nullptr) {
        input_version_->Unref();
        input_version_ = nullptr;
    }
}

}  // namespace leveldb
