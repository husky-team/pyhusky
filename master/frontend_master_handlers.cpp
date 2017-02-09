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

#include "master/frontend_master_handlers.hpp"

namespace husky {

FrontendMasterHandlers::FrontendMasterHandlers() { pyhusky_master_handlers.parent = this; }

int FrontendMasterHandlers::dag_split(const OpDAG& dag, int task_id) {
    int num_jobs = 0;
    for (const auto& i : dag.get_leaves())
        num_jobs += OperationSplitter::op_split(i, pending_jobs, task_id);
    return num_jobs;
}

void FrontendMasterHandlers::init_master() { pyhusky_master_handlers.init_master(); }

}  // namespace husky
