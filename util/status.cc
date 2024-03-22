// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/status.h"

#include <cstdio>

#include "port/port.h"

namespace leveldb {

/**
 * @brief
  //    state_[0..3] == length of message --> sizeof(uint32_t)
  //    state_[4]    == code
  //    state_[5..]  == message
 *
 * @param state
 * @return const char*
 */
const char* Status::CopyState(const char* state) {
  // 1. 先获取state的长度
  uint32_t size;
  std::memcpy(&size, state, sizeof(size));

  // 2. 再将state全部拷贝
  char* result = new char[size + 5];
  std::memcpy(result, state, size + 5);
  return result;
}

/**
 * @brief Construct a new Status:: Status object
 *
 * @param code
 * @param msg
 * @param msg2
 */
Status::Status(Code code, const Slice& msg, const Slice& msg2) {
  /// 1.判断状态码必须是错误码
  assert(code != kOk);

  // 2.计算提示信息size大小
  const uint32_t len1 = static_cast<uint32_t>(msg.size());
  const uint32_t len2 = static_cast<uint32_t>(msg2.size());
  const uint32_t size = len1 + (len2 ? (2 + len2) : 0);

  // 3.分配状态码所需要的空间 5 + msg
  char* result = new char[size + 5];

  // 4. 设置msg长度
  std::memcpy(result, &size, sizeof(size));

  // 5. 设置状态码
  result[4] = static_cast<char>(code);

  // 6. 拷贝msg到result[5]中
  std::memcpy(result + 5, msg.data(), len1);

  // 7. 拷贝msg2（详细信息）
  if (len2) {
    result[5 + len1] = ':';
    result[6 + len1] = ' ';
    std::memcpy(result + 7 + len1, msg2.data(), len2);
  }

  state_ = result;
}

std::string Status::ToString() const {
  if (state_ == nullptr) {
    return "OK";
  } else {
    char tmp[30];
    const char* type;
    switch (code()) {
      case kOk:
        type = "OK";
        break;
      case kNotFound:
        type = "NotFound: ";
        break;
      case kCorruption:
        type = "Corruption: ";
        break;
      case kNotSupported:
        type = "Not implemented: ";
        break;
      case kInvalidArgument:
        type = "Invalid argument: ";
        break;
      case kIOError:
        type = "IO error: ";
        break;
      default:
        std::snprintf(tmp, sizeof(tmp),
                      "Unknown code(%d): ", static_cast<int>(code()));
        type = tmp;
        break;
    }
    std::string result(type);
    uint32_t length;
    std::memcpy(&length, state_, sizeof(length));
    result.append(state_ + 5, length);
    return result;
  }
}

}  // namespace leveldb
