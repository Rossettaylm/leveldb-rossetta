#include <bits/chrono.h>
#include <chrono>
#include <ios>
#include <iostream>
#include <ostream>
#include <string>
#include "leveldb/slice.h"
#include "log_writer.h"

using namespace std;


int	main(int argc, char **argv)
{
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
