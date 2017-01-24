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

#include "master_handler.hpp"

#include "husky/base/log.hpp"
#include "husky/core/constants.hpp"
#include "husky/core/context.hpp"

#include "backend/splitter_register.hpp"
#include "manager/frontend_master_handlers.hpp"
#include "manager/job.hpp"
#include "manager/optimizer.hpp"

namespace husky {

void PyHuskyMasterHandlers::init_master() {
    // Gather information from master

    parent->daemon_generation.resize(Context::get_config().get_num_machines());

    // Register handlers with master
    Master::get_instance().register_main_handler(TYPE_SESSION_BEGIN_PY,
                                                 std::bind(&PyHuskyMasterHandlers::session_begin_handler, this));
    Master::get_instance().register_main_handler(TYPE_SESSION_END_PY,
                                                 std::bind(&PyHuskyMasterHandlers::session_end_handler, this));
    Master::get_instance().register_main_handler(TYPE_NEW_TASK,
                                                 std::bind(&PyHuskyMasterHandlers::new_task_handler, this));
    Master::get_instance().register_main_handler(TYPE_QUERY_TASK,
                                                 std::bind(&PyHuskyMasterHandlers::query_task_handler, this));
    Master::get_instance().register_main_handler(TYPE_TASK_END,
                                                 std::bind(&PyHuskyMasterHandlers::task_end_handler, this));
    Master::get_instance().register_main_handler(TYPE_REQ_INSTR,
                                                 std::bind(&PyHuskyMasterHandlers::request_instruction_handler, this));

    // Register library splitter
    splitter_register();
}

void PyHuskyMasterHandlers::session_begin_handler() {
    if (not py_started) {
        py_started = true;
        BinStream reply;
        reply << std::string("session_begin_py");
        parent->pending_jobs.push_back(std::make_shared<Job>(std::move(reply)));
    }
}

void PyHuskyMasterHandlers::session_end_handler() {
    if (py_started) {
        py_started = false;
        BinStream reply;
        reply << std::string("session_end_py");
        parent->pending_jobs.push_back(std::make_shared<Job>(std::move(reply)));
    }
}

void PyHuskyMasterHandlers::new_task_handler() {
    auto& master = Master::get_instance();
    auto master_handler = master.get_socket();
    auto bin_dag = zmq_recv_binstream(master_handler.get());
    OpDAG dag;
    unsigned int task_id;
    bin_dag >> dag >> task_id;
    unsigned int tot = parent->dag_split(Optimizer::optimize(dag), task_id);
    if (tot > 0) {
        parent->task_progress[task_id] = Progress{0, tot * Context::get_worker_info().get_num_workers()};
    }
}

void PyHuskyMasterHandlers::query_task_handler() {
    auto& master = Master::get_instance();
    auto master_handler = master.get_socket();
    auto binstream = zmq_recv_binstream(master_handler.get());
    unsigned int task_id;
    binstream >> task_id;
    BinStream reply;
    if (not task_results.empty()) {
        reply << std::string("data") << task_results.front().first << task_results.front().second;
        task_results.pop_front();
    } else {
        reply << std::string("progress") << parent->get_progress(task_id);
    }
    zmq_sendmore_string(master_handler.get(), master.get_cur_client());
    zmq_sendmore_dummy(master_handler.get());
    zmq_send_binstream(master_handler.get(), reply);
}

void PyHuskyMasterHandlers::task_end_handler() {
    auto& master = Master::get_instance();
    auto master_handler = master.get_socket();
    BinStream bin_proc_id = zmq_recv_binstream(master_handler.get());
    int proc_id = -1;
    bin_proc_id >> proc_id;
    while (bin_proc_id.size()) {
        std::pair<int, std::string> task_result;
        bin_proc_id >> task_result.first >> task_result.second;
        task_results.push_back(std::move(task_result));
    }
    int idx = (parent->daemon_generation[proc_id]++) - parent->cur_generation;
    parent->task_progress[parent->pending_jobs[idx]->get_task_id()].worker_finished +=
        Context::get_worker_info().get_num_local_workers(proc_id);
    if (parent->pending_jobs[idx]->inc_num_working_daemons() == Context::get_config().get_num_machines() && idx == 0) {
        parent->cur_generation++;
        auto& job = parent->pending_jobs.front();
        job->dec_dependency(parent->pending_jobs);
        parent->pending_jobs.pop_front();
    }
    zmq_sendmore_string(master_handler.get(), master.get_cur_client());
    zmq_sendmore_dummy(master_handler.get());
    zmq_send_dummy(master_handler.get());
}

void PyHuskyMasterHandlers::request_instruction_handler() {
    auto& master = Master::get_instance();
    auto master_handler = master.get_socket();
    BinStream bin_proc_id = zmq_recv_binstream(master_handler.get());
    int proc_id = -1;
    bin_proc_id >> proc_id;
    zmq_sendmore_string(master_handler.get(), master.get_cur_client());
    zmq_sendmore_dummy(master_handler.get());
    bool no_job = parent->pending_jobs.empty();  // no pending job at all
    if (not no_job) {
        int idx = parent->daemon_generation[proc_id] - parent->cur_generation;
        no_job = idx == parent->pending_jobs.size();  // this daemon did all pending jobs already
        if (not no_job) {
            time_t now = time(0);
            std::string msg = std::string("send new job at ") + ctime(&now);
            msg.back() = '.';  // \n => .
            zmq_send_binstream(master_handler.get(), parent->pending_jobs[idx]->to_bin_stream());
            return;
        }
    }
    if (no_job) {
        BinStream reply;
        zmq_send_binstream(master_handler.get(), reply);
    }
}

}  // namespace husky
