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

#include "manager/job.hpp"

#include <deque>
#include <string>

namespace husky {
#define Ptr std::shared_ptr

const std::shared_ptr<OpNode>& Job::get_op_tree_root() const { return op_tree.get_leaves().front(); }

Job::Job(Ptr<OpNode> node, unsigned _task_id) : dep_cnt(0), task_id(_task_id) {
    int ln = node->get_op().get_name().length();
    suffix = node->get_op().get_name().substr(ln - 3, ln);
    op_tree.add_leaf(node);
    op_tree.print();
}

bool Job::has_op_tree() const { return !op_tree.get_leaves().empty(); }

// if necessary we can make Job as a base class and generate two sub classes
Job::Job(BinStream&& s, unsigned _task_id) : stream(std::move(s)), task_id(_task_id) {}

unsigned Job::get_task_id() { return task_id; }

void Job::dec_dependency(std::deque<Ptr<Job>>& job_deque) {
    for (auto& i : deps) {
        if (--(i->dep_cnt) == 0)
            job_deque.push_back(i);
    }
}

void Job::add_dependency(const Ptr<Job>& b) {
    if (b != nullptr) {
        deps.push_back(b);
        b->dep_cnt++;
    }
}

bool Job::is_serialized() { return stream.size() != 0; }

BinStream& Job::to_bin_stream() {
    if (stream.size() == 0) {
        if (serializers.find(suffix) == serializers.end()) {
            stream << ("new_instr" + suffix);
            stream << op_tree;
        } else {
            stream = serializers[suffix](*this);
        }
    }
    return stream;
}

unsigned Job::inc_num_working_daemons() { return ++num_workers_daemons; }

std::unordered_map<std::string, std::function<BinStream(Job&)>> Job::serializers;

#undef Ptr
}  // namespace husky
