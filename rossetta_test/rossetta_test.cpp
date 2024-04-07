#include <bits/chrono.h>

#include <chrono>
#include <cstdint>
#include <ios>
#include <iostream>
#include <ostream>
#include <string>

#include "leveldb/slice.h"
#include "log_writer.h"

using namespace std;

char* EncodeVarint32(char* dst, uint32_t v) {
    // Operate on characters as unsigneds
    uint8_t* ptr = reinterpret_cast<uint8_t*>(dst);
    static const int B = 128;  // 2^7  0b10000000 0x80
    if (v < (1 << 7)) {
        *(ptr++) = v;
    } else if (v < (1 << 14)) {
        *(ptr++) = v | B;
        *(ptr++) = v >> 7;
    } else if (v < (1 << 21)) {
        *(ptr++) = v | B;
        *(ptr++) = (v >> 7) | B;
        *(ptr++) = v >> 14;
    } else if (v < (1 << 28)) {
        *(ptr++) = v | B;
        *(ptr++) = (v >> 7) | B;
        *(ptr++) = (v >> 14) | B;
        *(ptr++) = v >> 21;
    } else {
        *(ptr++) = v | B;
        *(ptr++) = (v >> 7) | B;
        *(ptr++) = (v >> 14) | B;
        *(ptr++) = (v >> 21) | B;
        *(ptr++) = v >> 28;
    }
    return reinterpret_cast<char*>(ptr);
}

int main(int argc, char** argv) {
    uint32_t value = 0x00353300;
    char dst[5];
    char* ptr = EncodeVarint32(dst, value);
    std::string str(dst, ptr - dst);

    cout << str << endl;

    return 0;
}
