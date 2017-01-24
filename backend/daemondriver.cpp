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

#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "boost/thread.hpp"

#include "husky/base/log.hpp"
#include "husky/base/session_local.hpp"
#include "husky/core/config.hpp"
#include "husky/core/constants.hpp"
#include "husky/core/context.hpp"
#include "husky/core/mailbox.hpp"
#include "husky/core/worker_info.hpp"

#include "backend/daemondriver.hpp"
#include "backend/workerdriver.hpp"

namespace husky {

std::unordered_map<std::string, std::function<void(DaemonInfo&)>> DaemonDriver::daemon_handler_map;

// init config
bool DaemonDriver::init_with_args(int ac, char** av, const std::vector<std::string>& customized) {
    // Context::init_global();
    Config config;
    WorkerInfo worker_info;

    bool success = config.init_with_args(ac, av, customized, &worker_info);
    if (success) {
        if (!config.get_log_dir().empty())
            base::log_to_dir(config.get_log_dir());
        Context::set_config(std::move(config));
        Context::set_worker_info(std::move(worker_info));
    }
    return success;
}

void DaemonDriver::daemon_run(int argc, char** argv, std::vector<std::string> args) {
    if (init_with_args(argc, argv, args)) {
        DaemonInfo daemon_info(argv[2]);
        daemon_info.init_param_ac = argc;
        daemon_info.init_param_av = argv;
        daemon_info.init_param_args = args;

        Context::create_mailbox_env();
        // Initialize coordinator
        Context::get_coordinator()->serve();

        while (true) {
            // ask master task
            BinStream bin_proc_id;
            bin_proc_id << Context::get_worker_info().get_process_id();
            daemon_info.dag_bin = Context::get_coordinator()->ask_master(bin_proc_id, TYPE_REQ_INSTR);

            if (daemon_info.dag_bin.size() == 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }

            daemon_info.dag_bin >> daemon_info.instr;

            // handle the task receive from master
            if (DaemonDriver::daemon_handler_map.find(daemon_info.instr) != DaemonDriver::daemon_handler_map.end()) {
                DaemonDriver::daemon_handler_map[daemon_info.instr](daemon_info);
            } else {
                throw std::runtime_error("Weird message received in daemon");
            }
        }
    }
}

void DaemonDriver::init_daemon_handler_map() {
    daemon_handler_map["session_begin_py"] = session_begin;
    daemon_handler_map["session_end_py"] = session_end;
    daemon_handler_map["new_instr_py"] = new_instr_py;
}

void DaemonDriver::add_new_handler(const std::string& key, std::function<void(DaemonInfo&)> handler) {
    daemon_handler_map[key] = handler;
}

void DaemonDriver::start_workers(DaemonInfo& daemon_info) {
    daemon_info.thread_connector.start();

    zmq::context_t& zmq_context = *(Context::get_zmq_context());
    const WorkerInfo& worker_info = Context::get_worker_info();

    base::SessionLocal::initialize();
    // Initialize worker threads
    int local_id = 0;
    for (int i = 0; i < worker_info.get_num_workers(); i++) {
        if (worker_info.get_process_id(i) != worker_info.get_process_id())
            continue;

        auto& to_daemon = daemon_info.thread_connector.new_itc_worker(local_id);
        daemon_info.threads.push_back(new boost::thread([=, &zmq_context, &daemon_info, &to_daemon]() {
            Context::set_local_tid(local_id);
            Context::set_global_tid(i);

            // run the job
            WorkerDriverInfo workerdriver_info(local_id, i, worker_info.get_num_workers(), to_daemon);
            WorkerDriver::worker_run(workerdriver_info);
        }));
        local_id += 1;
    }

    daemon_info.thread_connector.broadcast_to_threads(daemon_info.instr);
}

void DaemonDriver::session_begin(DaemonInfo& daemon_info) {
    int proc_id = Context::get_worker_info().get_process_id();
    if (proc_id == -1) {
        throw std::runtime_error("proc_id error");
    }

    start_workers(daemon_info);

    BinStream bin_proc_id;
    bin_proc_id << Context::get_worker_info().get_process_id();
    Context::get_coordinator()->ask_master(bin_proc_id, TYPE_TASK_END);
}

void DaemonDriver::session_end(DaemonInfo& daemon_info) {
    daemon_info.thread_connector.broadcast_to_threads(daemon_info.instr);

    for (int i = 0; i < daemon_info.threads.size(); i++) {
        daemon_info.threads[i]->join();
        delete daemon_info.threads[i];
    }

    daemon_info.threads.clear();
    daemon_info.thread_connector.close();

    base::SessionLocal::thread_finalize();

    // to init the context again to make sure to continue receiving new task
    // init conf first
    if (init_with_args(daemon_info.init_param_ac, daemon_info.init_param_av, daemon_info.init_param_args)) {
        Context::get_coordinator()->serve();
    }

    BinStream bin_proc_id;
    bin_proc_id << Context::get_worker_info().get_process_id();
    Context::get_coordinator()->ask_master(bin_proc_id, TYPE_TASK_END);
}

void DaemonDriver::new_instr_py(DaemonInfo& daemon_info) {
    daemon_info.thread_connector.broadcast_to_threads(daemon_info.instr);
    daemon_info.thread_connector.broadcast_to_threads(daemon_info.dag_bin);
    // wait for workers ...
    // tell master
    BinStream result_buf;
    result_buf << Context::get_worker_info().get_process_id();

    daemon_info.thread_connector.listen_from_threads(result_buf);
    Context::get_coordinator()->ask_master(result_buf, TYPE_TASK_END);
}

}  // namespace husky
