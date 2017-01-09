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

#include "operation.hpp"

#include <map>
#include <string>

namespace husky {

BinStream& operator>>(BinStream& stream, Operation& op) {
    stream >> op.op_name;
    size_t len;
    std::string k, v;
    stream >> len;
    for (int i = 0; i < len; ++i) {
        stream >> k >> v;
        op.op_param[k] = v;
    }
    return stream;
}

BinStream& operator<<(BinStream& stream, Operation& op) {
    stream << op.op_name;
    stream << op.op_param.size();
    for (auto& kv : op.op_param) {
        stream << kv.first << kv.second;
    }
    return stream;
}

Operation::Operation() {}

Operation::Operation(const std::string& name):op_name(name) {}

Operation::Operation(const std::string& name,
    const std::map<std::string, std::string>& params):op_name(name), op_param(params) {}

Operation::Operation(const Operation& op):op_name(op.op_name), op_param(op.op_param) {}

}  // namespace husky
