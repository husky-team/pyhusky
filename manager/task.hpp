// Copyright 2016 Husky Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "husky/base/serialization.hpp"

#include "manager/operation.hpp"

namespace husky {

using base::BinStream;

class Task;
BinStream& operator>>(BinStream& stream, Task& task);

class Task {
   public:
    friend BinStream& operator>>(BinStream& stream, Task& task);
    size_t get_num_operations() { return op_list.size(); }
    Operation& get_operation(int idx) { return op_list[idx]; }

   protected:
    std::vector<Operation> op_list;
};

}  // namespace husky
