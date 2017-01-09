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

#include <unordered_map>

#include "backend/threadconnector.hpp"

namespace husky {

class PythonConnector;
struct WorkerDriverInfo {
    WorkerDriverInfo(int lid, int gid, int _num_workers, ITCWorker & _to_daemon):
        local_worker_id(lid), global_worker_id(gid), num_workers(_num_workers), to_daemon(_to_daemon) {}

    std::string instr;
    int global_worker_id;
    int local_worker_id;
    int num_workers;
    ITCWorker & to_daemon;
    PythonConnector * py_connector = nullptr;
};

class OpDAG;
class PythonSocket;
class Operation;
class WorkerDriver {
public:
    // Register operator handler
    static void register_handler();
    static void add_handler(const std::string & name,
            std::function<void(const Operation& op, PythonSocket& python_socket, ITCWorker& daemon_socket)> handler);
    // Register instr handler
    static void init_worker_instr_handler_map();
    static void add_handler(const std::string & key, const std::function<bool(WorkerDriverInfo&)> & handler);
    static void worker_run(WorkerDriverInfo & workerdriver_info);
private:
    // operators
    static bool session_begin_py(WorkerDriverInfo& workerdriver_info);
    static bool session_end_py(WorkerDriverInfo& workerdriver_info);
    static bool new_instr_py(WorkerDriverInfo& workerdriver_info);

    static bool new_instr_python_py(WorkerDriverInfo& workerdriver_info, OpDAG & opdag);
    static bool new_instr_cpp_py(WorkerDriverInfo& workerdriver_info, OpDAG & opdag);

    static bool check_instr(OpDAG & opdag);

    static std::unordered_map<std::string, 
        std::function<bool(WorkerDriverInfo&)>> worker_instr_handler_map;

    static std::unordered_map<std::string,
        std::function<void(const Operation& op, PythonSocket& python_socket, ITCWorker& daemon_socket)>> worker_operator_handler_map;
};
}  // namespace husky
