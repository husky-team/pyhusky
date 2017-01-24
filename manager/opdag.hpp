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

#include <iostream>
#include <memory>
#include <vector>

#include "manager/operation.hpp"

namespace husky {

using base::BinStream;

class Optimizer;
class OpDAG;
class OpNode;
BinStream& operator>>(BinStream& stream, OpDAG& dag);
BinStream& operator<<(BinStream& stream, OpDAG& dag);
int _visit_deps(OpNode& node, BinStream& stream, int& id_counter);

class OpNode {
   public:
    OpNode();
    explicit OpNode(const Operation& oper, int _id = -1);

    friend class OpDAG;
    friend class Optimizer;
    friend class TestOp;
    int get_id() const;
    std::vector<std::shared_ptr<OpNode>>& get_deps();
    const std::vector<std::shared_ptr<OpNode>>& get_deps() const;
    Operation& get_op();
    const Operation& get_op() const;
    friend BinStream& operator>>(BinStream& stream, OpDAG& dag);
    friend BinStream& operator<<(BinStream& stream, OpDAG& dag);
    friend int _visit_deps(OpNode& node, BinStream& stream, int& id_counter);

   protected:
    Operation op;
    std::vector<std::shared_ptr<OpNode>> deps;
    int id = -1;
};

class OpDAG {
   public:
    friend class Optimizer;
    friend class TestOp;
    void print();
    std::vector<std::shared_ptr<OpNode>> const& get_leaves() const;
    friend BinStream& operator>>(BinStream& stream, OpDAG& dag);
    friend BinStream& operator<<(BinStream& stream, OpDAG& dag);
    OpDAG deepcopy() const;
    void add_leaf(const std::shared_ptr<OpNode>& leaf);

   protected:
    std::unordered_map<int, std::shared_ptr<OpNode>> op_nodes;
    std::vector<std::shared_ptr<OpNode>> leaves;
};

}  // namespace husky
