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
#include <memory>
#include <string>
#include <unordered_map>

#include "backend/opdag.hpp"
#include "master/job.hpp"

namespace husky {

class OperationSplitter {
   private:
#define Ptr std::shared_ptr
#define new_ptr std::make_shared

    typedef unsigned (*OpSplitterType)(const Ptr<OpNode>&, const Ptr<OpNode>&);

    static unsigned cur_task_id;
    static std::deque<Ptr<Job>>* cur_task_queue;
    static Ptr<Job> last_created_task;

    static std::unordered_map<std::string, OpSplitterType> op_splitter_map;

   public:
    static void add_splitter(const std::string& name, const OperationSplitter::OpSplitterType& splitter);

    static unsigned _default(const Ptr<OpNode>& op, const Ptr<OpNode>& son);

    /**
     * Add the job into the pending queue.
     **/
    static unsigned load(const Ptr<OpNode>& op, const Ptr<OpNode>& son);

    /**
     * Split the `op` into two operations, sharing the same parameters.
     * If the original name of `op` is `op_name`, the names for the two operations are `op_name` and `op_name_end`
     * The job with the operation of name `op_name_end` will depends on the other on
     **/
    static unsigned simple_split(const Ptr<OpNode>& op, const Ptr<OpNode>& son);

    static unsigned difference(const Ptr<OpNode>& op, const Ptr<OpNode>& son);

    static unsigned handle(const Ptr<OpNode>& op, const Ptr<OpNode>& sons = nullptr);

   public:
    static unsigned op_split(const Ptr<OpNode>& op, std::deque<Ptr<Job>>& job_deque, unsigned task_id);

#undef Ptr
#undef new_ptr
};

}  // namespace husky
