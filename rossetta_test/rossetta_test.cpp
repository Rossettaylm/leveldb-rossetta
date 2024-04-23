#include <bits/chrono.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <ios>
#include <iostream>
#include <ostream>
#include <string>
#include <vector>

#include "leveldb/slice.h"
#include "log_writer.h"

using namespace std;

void trans(const int * array, int n) {
    for (int i = 0; i < n; ++i) {
        printf("%d ", array[i]);
    }
    printf("\n");
}


int main(int argc, char** argv) {
    vector<int> values{3, 5, 3, 3, 10, 100, 0};

    // trans(&values[0], values.size());
    int *pvalue = nullptr;
    delete pvalue;

    return 0;
}
