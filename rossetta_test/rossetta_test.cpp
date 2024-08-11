#include <leveldb/db.h>
#include <unistd.h>

#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <random>
#include <vector>

#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "leveldb/status.h"

using namespace std;
using namespace leveldb;

int MyLeveldbTest() {
    Options options;
    options.create_if_missing = true;

    const std::string dbname = "rossetta_test_db";
    DB* rossetta_test_db = nullptr;
    Status status = DB::Open(options, dbname, &rossetta_test_db);
    if (status.ok()) {
        cout << "dingji" << endl;
        WriteOptions write_options;
        write_options.sync = false;

        ReadOptions read_options;
        read_options.fill_cache = true;

        rossetta_test_db->Put(write_options, "1", "queshidingji");
        rossetta_test_db->Put(write_options, "2", "daraole");
        rossetta_test_db->Put(write_options, "3", "zhidaole");
        rossetta_test_db->Delete(write_options, "1");

        Iterator* db_iter = rossetta_test_db->NewIterator(read_options);
        for (db_iter->SeekToFirst(); db_iter->Valid(); db_iter->Next()) {
            cout << db_iter->key().ToString() << " "
                 << db_iter->value().ToString() << endl;
        }
        delete db_iter;
    }

    delete rossetta_test_db;

    return 0;
}

void printArgs(int arg_count, ...) {
    std::va_list va;
    va_start(va, arg_count);
    for (int i = 0; i < arg_count; ++i) {
        cout << va_arg(va, const char*) << endl;
    }
    va_end(va);
}

struct Values {
    std::vector<int> values;

    Values() { memset(this, 0x00, sizeof(Values)); }
};

int main() {
    // printArgs(2, "dingshi", "dingshi", "nihao");

    // int re = MyLeveldbTest();

    return 0;
}