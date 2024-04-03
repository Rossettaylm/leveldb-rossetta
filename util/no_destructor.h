// Copyright (c) 2018 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_NO_DESTRUCTOR_H_
#define STORAGE_LEVELDB_UTIL_NO_DESTRUCTOR_H_

#include <type_traits>
#include <utility>

namespace leveldb {

// Wraps an instance whose destructor is never called.
//
// This is intended for use with function-level static variables.
// TODO(rossetta) 2024-04-03 16:38:27 无析构函数的模板
template <typename InstanceType>
class NoDestructor {
  public:
    template <typename... ConstructorArgTypes>
    explicit NoDestructor(ConstructorArgTypes&&... constructor_args) {
        //? 通过static_assert判断分配内存和对齐方式
        static_assert(
            sizeof(instance_storage_) >= sizeof(InstanceType),
            "instance_storage_ is not large enough to hold the instance");
        static_assert(
            alignof(decltype(instance_storage_)) >= alignof(InstanceType),
            "instance_storage_ does not meet the instance's alignment "
            "requirement");
        //? placement new
        new (&instance_storage_) InstanceType(
            std::forward<ConstructorArgTypes>(constructor_args)...);
    }

    ~NoDestructor() = default;

    NoDestructor(const NoDestructor&) = delete;
    NoDestructor& operator=(const NoDestructor&) = delete;

    InstanceType* get() {
        return reinterpret_cast<InstanceType*>(&instance_storage_);
    }

  private:
    //? 分配对齐内存空间
    typename std::aligned_storage<
        sizeof(InstanceType), alignof(InstanceType)>::type instance_storage_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_NO_DESTRUCTOR_H_
