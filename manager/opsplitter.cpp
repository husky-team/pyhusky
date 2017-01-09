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

#include "opsplitter.hpp"

#include <deque>
#include <string>


namespace husky {
#define Ptr std::shared_ptr
#define new_ptr std::make_shared

void OperationSplitter::add_splitter(const std::string& name, const OperationSplitter::OpSplitterType& splitter) {
    op_splitter_map.insert(std::make_pair(name, splitter));
}

unsigned OperationSplitter::_default(const Ptr<OpNode>& op, const Ptr<OpNode>& son) {
    Ptr<OpNode> new_op = new_ptr<OpNode>(op->get_op(), op->get_id());
    if (son)
        new_op->get_deps().push_back(son);
    unsigned add = 0;
    for (const auto& i : op->get_deps())
        add += handle(i, new_op);
    return add;
}

unsigned OperationSplitter::load(const Ptr<OpNode>& op, const Ptr<OpNode>& son) {
    Ptr<OpNode> new_op = new_ptr<OpNode>(op->get_op(), op->get_id());
    if (son)
        new_op->get_deps().push_back(son);
    Ptr<Job> job = new_ptr<Job>(new_op, cur_task_id);
    cur_task_queue->push_back(job);
    last_created_task = job;
    return 1;
}

unsigned OperationSplitter::simple_split(const Ptr<OpNode>& op, const Ptr<OpNode>& son) {
    std::string name = op->get_op().get_name();
    int prefix_len = name.length() - 3;
    std::string suffix = name.substr(prefix_len);
    Ptr<OpNode> send = new_ptr<OpNode>(op->get_op());
    Ptr<OpNode> recv = new_ptr<OpNode>(
        Operation(name.substr(0, prefix_len) + "_end" + suffix, op->get_op().get_params()),
        op->get_id());

    if (son != nullptr)
        recv->get_deps().push_back(son);
    Ptr<Job> recv_job = new_ptr<Job>(recv, cur_task_id);

    unsigned add = 1, new_add;
    for (const auto& i : op->get_deps()) {
        new_add = handle(i, send);
        if (new_add)
            last_created_task->add_dependency(recv_job);
        add += new_add;
    }
    if (add > 1)
        last_created_task = recv_job;
    return add > 1 ? add : 0;
}

unsigned OperationSplitter::difference(const Ptr<OpNode>& op, const Ptr<OpNode>& son) {
    std::string op_name = op->get_op().get_name();
    std::string suffix = op_name.substr(op->get_op().get_name().length() - 3);
    std::string self_name = op->get_op().get_param(suffix == "_py" ? "list_name":"listName");

    unsigned add = 1;
    auto params = op->get_op().get_params();
    params[self_name + "_diff_type"] = "left";
    Ptr<OpNode> to_left = new_ptr<OpNode>(
        Operation((suffix == "_py" ? "Functional#difference":"difference") + suffix, params));
    params[self_name + "_diff_type"] = "right";
    Ptr<OpNode> to_right = new_ptr<OpNode>(
        Operation((suffix == "_py" ? "Functional#difference":"difference") + suffix, params));
    Ptr<OpNode> recv = new_ptr<OpNode>(
        Operation(op_name.substr(0, op_name.length() - 3) + "_end" + suffix, op->get_op().get_params()),
        op->get_id());
    recv->get_deps().push_back(son);

    Ptr<Job> recv_job = new_ptr<Job>(recv, cur_task_id);
    for (const auto& i : op->get_deps()) {
        unsigned new_add = 0;
        const std::string& left = i->get_op().get_param_or(self_name + "_diffl", "");
        const std::string& right = i->get_op().get_param_or(self_name + "_diffr", "");
        if (!right.empty()) {
            new_add = handle(i, to_right);
            if (new_add)
                last_created_task->add_dependency(recv_job);
        } else if (!left.empty()) {
            new_add = handle(i, to_left);
            if (new_add)
                last_created_task->add_dependency(recv_job);
        } else {
            assert(false);
        }
        add += new_add;
    }

    if (add > 1)
        last_created_task = recv_job;
    return add > 1 ? add : 0;
}


unsigned OperationSplitter::handle(const Ptr<OpNode>& op, const Ptr<OpNode>& sons) {
    const auto& iter = op_splitter_map.find(op->get_op().get_name());
    last_created_task = nullptr;
    if (iter == op_splitter_map.end())
        return _default(op, sons);
    else
        return iter->second(op, sons);
}

unsigned OperationSplitter::op_split(const Ptr<OpNode>& op, std::deque<Ptr<Job>>& job_deque, unsigned task_id) {
    cur_task_id = task_id;
    cur_task_queue =&job_deque;
    return handle(op);
}

std::unordered_map<std::string, OperationSplitter::OpSplitterType> OperationSplitter::op_splitter_map;
unsigned OperationSplitter::cur_task_id = -1;
std::deque<Ptr<Job>>* OperationSplitter::cur_task_queue = nullptr;
Ptr<Job> OperationSplitter::last_created_task = nullptr;
#undef Ptr
#undef new_ptr

}  // namespace husky
