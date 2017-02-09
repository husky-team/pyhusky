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

#include <deque>
#include <list>
#include <memory>
#include <string>
#include <vector>

#include "husky/base/serialization.hpp"

#include "backend/opdag.hpp"

namespace husky {

class Job {
   private:
#define Ptr std::shared_ptr
    static std::unordered_map<std::string, std::function<BinStream(Job&)>> serializers;

    BinStream stream;
    unsigned num_workers_daemons = 0;
    std::vector<Ptr<Job>> deps;
    unsigned dep_cnt;
    OpDAG op_tree;
    std::string suffix;

    unsigned task_id;

   public:
    static void add_serializer(const std::string& suffix, std::function<BinStream(Job&)> serializer) {
        serializers.insert(std::make_pair(suffix, serializer));
    }

    bool has_op_tree() const;

    const std::shared_ptr<OpNode>& get_op_tree_root() const;

    Job(Ptr<OpNode> node, unsigned _task_id);

    explicit Job(BinStream&& s, unsigned _task_id = -1);

    void set_op_node(Ptr<OpNode>& node);

    unsigned get_task_id();

    void dec_dependency(std::deque<Ptr<Job>>& job_deque);

    void add_dependency(const Ptr<Job>& b);

    BinStream& to_bin_stream();

    bool is_serialized();

    unsigned inc_num_working_daemons();

#undef Ptr
};

}  // namespace husky
