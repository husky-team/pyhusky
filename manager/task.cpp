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

#include "manager/task.hpp"

namespace husky {

BinStream& operator>>(BinStream& stream, Task& task) {
    size_t len;
    stream >> len;
    for (int i = 0; i < len; i++) {
        Operation op;
        stream >> op;
        task.op_list.push_back(op);
    }
}

}  // namespace husky
