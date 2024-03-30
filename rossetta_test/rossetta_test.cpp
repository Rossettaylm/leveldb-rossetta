#include <bits/chrono.h>
#include <chrono>
#include <ios>
#include <iostream>
#include <ostream>
#include <string>
#include "leveldb/slice.h"
#include "log_writer.h"

using namespace std;

struct S
{
    char a;     // memory location #1
    int b : 5;  // memory location #2
    int c : 11, // memory location #2 (continued)
          : 0,
        d : 8;  // memory location #3
    struct
    {
        int ee : 8; // memory location #4
    } e;
} obj; // The object 'obj' consists of 4 separate memory locations

// #pragma pack(8)

struct Test {
    int aa;
    double bb;
};

struct AA {
    char bb;  // 0
    Test test;// 8  min(8, sizeof(Test))
    double aa;// 24
    int value : 3; // 32
    int valueb : 7;
    const char* c; // 36
};


struct CC {
    int valuea : 3;
    int valueb : 7;
    int valuec : 4;
};


int	main(int argc, char **argv)
{

    constexpr size_t testsize = sizeof(Test);
    constexpr size_t aasize = sizeof(AA);
    constexpr size_t ccsize = sizeof(CC);
    AA tempvalue;
    tempvalue.value = 1;

  {
    string str(10000, 'a');

    auto begin = std::chrono::steady_clock::now();
    for (int i = 0; i < 10000; ++i) {
      string temp = str;
    }
    auto end = std::chrono::steady_clock::now();

    cout << chrono::duration_cast<chrono::milliseconds>(end - begin).count() << "ms" << endl;
  }

  {
    string str(10000, 'a');

    auto begin = std::chrono::steady_clock::now();
    for (int i = 0; i < 10000; ++i) {
      string temp = str;
      temp.replace(0, 1, 1, 'b');
    }
    auto end = std::chrono::steady_clock::now();

    cout << chrono::duration_cast<chrono::milliseconds>(end - begin).count() << "ms" << endl;
  }



  return 0;
}
