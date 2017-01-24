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

#include "manager/opdag.hpp"

#include <queue>
#include <vector>

namespace husky {

std::vector<std::shared_ptr<OpNode>>& OpNode::get_deps() { return deps; }

const std::vector<std::shared_ptr<OpNode>>& OpNode::get_deps() const { return deps; }

int OpNode::get_id() const { return id; }

Operation& OpNode::get_op() { return op; }

const Operation& OpNode::get_op() const { return op; }

const std::vector<std::shared_ptr<OpNode>>& OpDAG::get_leaves() const { return leaves; }

void OpDAG::add_leaf(const std::shared_ptr<OpNode>& leaf) { leaves.push_back(leaf); }

BinStream& operator>>(BinStream& stream, OpDAG& dag) {
    dag.op_nodes.clear();
    dag.leaves.clear();
    while (stream.size() != 0) {
        int id;
        stream >> id;
        if (id == -1)
            break;
        dag.op_nodes[id] = std::make_shared<OpNode>();
        std::shared_ptr<OpNode>& node = dag.op_nodes[id];
        node->id = id;
        stream >> node->op;
        std::vector<int> dep_ids;
        stream >> dep_ids;
        for (auto dep_id : dep_ids) {
            assert(dag.op_nodes.find(dep_id) != dag.op_nodes.end());
            node->deps.push_back(dag.op_nodes[dep_id]);
        }
    }

    std::vector<bool> is_leave;
    is_leave.resize(dag.op_nodes.size());
    std::fill(is_leave.begin(), is_leave.end(), true);
    for (auto& id_node : dag.op_nodes) {
        auto& node = id_node.second;
        for (auto dep : node->get_deps())
            is_leave[dep->id] = false;
    }
    for (int i = 0; i < is_leave.size(); i++)
        if (is_leave[i])
            dag.leaves.push_back(dag.op_nodes[i]);
    return stream;
}

int _visit_deps(OpNode& node, BinStream& stream, int& id_counter) {
    id_counter += 1;
    int id = id_counter;

    std::vector<int> self_dep_list;
    for (auto dep_ptr : node.get_deps()) {
        int dep_id = _visit_deps(*dep_ptr, stream, id_counter);
        self_dep_list.push_back(dep_id);
    }

    stream << id;
    stream << node.op;
    stream << self_dep_list;
    return id;
}

BinStream& operator<<(BinStream& stream, OpDAG& dag) {
    int id_counter = -1;
    assert(dag.leaves.size() == 1);  // FIXME
    _visit_deps(*(dag.leaves[0]), stream, id_counter);
    return stream;
}

OpDAG OpDAG::deepcopy() const {
    OpDAG ret;
    for (auto& kv : op_nodes) {
        std::shared_ptr<OpNode>& node = ret.op_nodes[kv.first];
        node = std::make_shared<OpNode>();
        node->id = kv.first;
        node->op = kv.second->op;
    }
    for (auto& kv : op_nodes) {
        auto& node = ret.op_nodes[kv.first];
        for (auto dep : kv.second->deps)
            node->deps.push_back(ret.op_nodes[dep->id]);
    }
    std::unordered_map<int, bool> is_leave;
    for (auto& id_node : ret.op_nodes) {
        auto& node = id_node.second;
        for (auto dep : node->get_deps())
            is_leave[dep->id] = false;
    }
    for (auto& kv : ret.op_nodes)
        if (is_leave.find(kv.first) == is_leave.end())
            ret.leaves.push_back(kv.second);
    return ret;
}

void OpDAG::print() {
    // for (auto& kv : op_nodes) {
    std::queue<std::shared_ptr<OpNode>> Q;
    for (auto& kv : leaves) {
        Q.push(kv);
    }
    while (!Q.empty()) {
        auto& kv = Q.front();
        std::cout << kv->get_op().get_name() << ": ";
        for (auto nb : kv->deps) {
            std::cout << nb->get_op().get_name() << " ";
            Q.push(nb);
        }
        std::cout << std::endl;
        Q.pop();
    }
    std::cout << std::endl;
}

OpNode::OpNode() {}

OpNode::OpNode(const Operation& oper, int _id) : op(oper), id(_id) {}

}  // namespace husky
