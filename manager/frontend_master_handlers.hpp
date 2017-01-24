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
#include <vector>

#include "backend/master_handler.hpp"
#include "manager/job.hpp"
#include "manager/opdag.hpp"
#include "manager/opsplitter.hpp"
#include "master/master.hpp"

namespace husky {

struct Progress {
    Progress() : worker_finished(0), num_tot_worker(0) {}
    Progress(unsigned int a, unsigned int b) : worker_finished(a), num_tot_worker(b) {}
    Progress(const Progress& p) : worker_finished(p.worker_finished), num_tot_worker(p.num_tot_worker) {}
    unsigned int worker_finished;
    unsigned int num_tot_worker;
};

class FrontendMasterHandlers {
   private:
    friend PyHuskyMasterHandlers;
    // master generation for tasks
    int cur_generation = 0;
    // generations of each daemon on each machine for tasks
    std::vector<int> daemon_generation;

   public:
    FrontendMasterHandlers();
    void init_master();
    int dag_split(const OpDAG& dag, int task_id);

   protected:
    std::deque<std::shared_ptr<Job>> pending_jobs;
    PyHuskyMasterHandlers pyhusky_master_handlers;
    std::unordered_map<int, Progress> task_progress;
    void inc_job_progress(int task_id) {
        auto iter = task_progress.find(task_id);
        if (iter != task_progress.end()) {
            auto& prog = iter->second;
            if (++prog.worker_finished == prog.num_tot_worker) {
                // once iter is removed, prog becomes invalid !
                task_progress.erase(iter);
            }
        }
    }
    int get_progress(int task_id) {
        auto iter = task_progress.find(task_id);
        if (iter != task_progress.end()) {
            auto& prog = iter->second;
            return static_cast<int>(100. * prog.worker_finished / prog.num_tot_worker);
        } else {
            return 100;
        }
    }
};

}  // namespace husky
