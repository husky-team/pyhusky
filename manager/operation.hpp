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

#include <map>
#include <string>

#include "husky/base/log.hpp"
#include "husky/base/serialization.hpp"


namespace husky {

using base::BinStream;

class Operation;
BinStream& operator>>(BinStream& stream, Operation& op);
BinStream& operator<<(BinStream& stream, Operation& op);

class Operation {
  private:
    friend class TestOp;
  public:
    Operation();
    explicit Operation(const std::string& name);
    explicit Operation(const Operation& op);
    Operation(const std::string& name, const std::map<std::string, std::string>& params);
    friend BinStream& operator>>(BinStream& stream, Operation& op);
    friend BinStream& operator<<(BinStream& stream, Operation& op);
    std::string const& get_name() const {
        return op_name;
    }
    std::string get_op_name() const {
        return op_name.substr(0, op_name.length() - 3);
    }
    std::string const& get_param(const std::string& key) const {
        try {
            return op_param.at(key);
        } catch (...) {
            base::log_msg("Unable to get value of " + key);
            assert(false);
        }
    }
    std::map<std::string, std::string> const& get_params() const {
        return op_param;
    }
    std::map<std::string, std::string>& get_params() {
        return op_param;
    }
    std::string const& get_param_or(const std::string& key, const std::string& val) const {
        const auto& iter = op_param.find(key);
        return iter == op_param.end() ? val : iter->second;
    }
    int get_param_size() {
        return op_param.size();
    }

  protected:
    std::string op_name;
    std::map<std::string, std::string> op_param;
};

}  // namespace husky
