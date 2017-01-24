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

#include "backend/workerdriver.hpp"

#include <string>

#include "husky/base/log.hpp"
#include "husky/base/serialization.hpp"
#include "husky/core/context.hpp"
#include "husky/core/executor.hpp"
#include "husky/core/utils.hpp"
#include "husky/core/zmq_helpers.hpp"
#include "husky/io/input/line_inputformat.hpp"
#ifdef WITH_MONGODB
#include "husky/io/input/mongodb_inputformat.hpp"
#endif

#include "backend/pythonconnector.hpp"
#include "backend/register.hpp"
#include "backend/threadconnector.hpp"
#include "manager/opdag.hpp"
#include "manager/operation.hpp"

namespace husky {

std::unordered_map<std::string, std::function<bool(WorkerDriverInfo&)>> WorkerDriver::worker_instr_handler_map;
std::unordered_map<std::string,
                   std::function<void(const Operation& op, PythonSocket& python_socket, ITCWorker& daemon_socket)>>
    WorkerDriver::worker_operator_handler_map;

void WorkerDriver::worker_run(WorkerDriverInfo& workerdriver_info) {
    while (true) {
        workerdriver_info.instr = workerdriver_info.to_daemon.recv_string();
        if (WorkerDriver::worker_instr_handler_map.find(workerdriver_info.instr) !=
            WorkerDriver::worker_instr_handler_map.end()) {
            bool exit = WorkerDriver::worker_instr_handler_map[workerdriver_info.instr](workerdriver_info);
            if (exit)
                break;
        } else {
            throw std::runtime_error("Weird message received in worker_run");
        }
    }
}
void WorkerDriver::init_worker_instr_handler_map() {
    worker_instr_handler_map["session_begin_py"] = session_begin_py;
    worker_instr_handler_map["session_end_py"] = session_end_py;
    worker_instr_handler_map["new_instr_py"] = new_instr_py;
}

void WorkerDriver::register_handler() {
    // init all the handlers
    RegisterFunction::register_cpp_handlers();
}

void WorkerDriver::add_handler(
    const std::string& name,
    std::function<void(const Operation& op, PythonSocket& python_socket, ITCWorker& daemon_socket)> handler) {
    assert(worker_operator_handler_map.find(name) == worker_operator_handler_map.end() &&
           "handler exists in workerdriver");
    worker_operator_handler_map[name] = handler;
}

void WorkerDriver::add_handler(const std::string& key, const std::function<bool(WorkerDriverInfo&)>& handler) {
    worker_instr_handler_map.insert(std::make_pair(key, handler));
}

bool WorkerDriver::session_begin_py(WorkerDriverInfo& workerdriver_info) {
    workerdriver_info.py_connector =
        new PythonConnector(workerdriver_info.to_daemon, workerdriver_info.local_worker_id);
    workerdriver_info.py_connector->start_python_proc(
        workerdriver_info.local_worker_id, workerdriver_info.global_worker_id, workerdriver_info.num_workers);
    return 0;
}
bool WorkerDriver::session_end_py(WorkerDriverInfo& workerdriver_info) {
    // free worker
    workerdriver_info.py_connector->send_string(workerdriver_info.instr);
    workerdriver_info.py_connector->close_python_proc();
    delete workerdriver_info.py_connector;
    // Context::finalize_local();
    LOG_I << "closed";
    return 1;
}
bool WorkerDriver::check_instr(OpDAG& opdag) {
    std::function<bool(std::shared_ptr<OpNode> node)> visit;
    visit = [&visit](std::shared_ptr<OpNode> node) {
        std::string type = node->get_op().get_param_or("Type", "py");
        if (type == "py")
            return false;
        bool ret = true;
        for (auto dep : node->get_deps()) {
            ret &= visit(dep);
        }
        return ret;
    };
    return visit(opdag.get_leaves()[0]);
}
bool WorkerDriver::new_instr_py(WorkerDriverInfo& workerdriver_info) {
    BinStream job_stream = workerdriver_info.to_daemon.recv_binstream();
    BinStream job_stream_bak = job_stream;
    OpDAG opdag;
    job_stream >> opdag;
    // check whether the whole instruction can be executed in c++ side
    bool is_cpp = check_instr(opdag);
    LOG_I << "Type:" + std::to_string(is_cpp);
    if (is_cpp) {
        return new_instr_cpp_py(workerdriver_info, opdag);
    } else {
        workerdriver_info.py_connector->send_string(workerdriver_info.instr);
        workerdriver_info.py_connector->send_binstream(job_stream_bak);
        return new_instr_python_py(workerdriver_info, opdag);
    }
}
bool WorkerDriver::new_instr_cpp_py(WorkerDriverInfo& workerdriver_info, OpDAG& opdag) {
    std::function<void(std::shared_ptr<OpNode> node)> visit;
    visit = [&visit, &workerdriver_info](std::shared_ptr<OpNode> node) {
        const std::string& operator_name = node->get_op().get_name();
        LOG_I << "visiting: " + operator_name;
        if (worker_operator_handler_map.find(operator_name) != worker_operator_handler_map.end()) {
            worker_operator_handler_map[operator_name](node->get_op(), workerdriver_info.py_connector->python_socket,
                                                       workerdriver_info.py_connector->daemon_socket);
        } else {
            throw std::runtime_error("Weird message received in WorkerDriver: " + operator_name);
        }
        for (auto dep : node->get_deps()) {
            visit(dep);
        }
    };
    visit(opdag.get_leaves()[0]);
    workerdriver_info.py_connector->daemon_socket.send("instr_end");
}
bool WorkerDriver::new_instr_python_py(WorkerDriverInfo& workerdriver_info, OpDAG& opdag) {
    LOG_I << "worker_run: start instr";
    ASSERT_MSG(opdag.get_leaves().size() == 1, "not single input");
    auto leave = opdag.get_leaves()[0];
    if (leave->get_op().get_name() == "Functional#load_py") {
        std::string protocol = leave->get_op().get_param("Protocol");
        if (protocol == "hdfs" || protocol == "nfs") {
            husky::io::LineInputFormat infmt;
            const std::string path = leave->get_op().get_param("Path");
            infmt.set_input(path);
            // buffered send
            std::string buf;
            unsigned num = 0;
            husky::load(infmt, [&](boost::string_ref& chunk) {
                buf.append(chunk.data(), chunk.size());
                if (++num == 1024) {
                    num = 0;
                    workerdriver_info.py_connector->send_string(buf);
                    buf.clear();
                    workerdriver_info.py_connector->recv_string();
                } else {
                    buf.push_back('\n');
                }
            });
            if (num != 0) {
                workerdriver_info.py_connector->send_string(buf);
                workerdriver_info.py_connector->recv_string();
            }
        } else if (protocol == "mongodb") {
#ifdef WITH_MONGODB
            husky::io::MongoDBInputFormat infmt;
            std::string server = leave->get_op().get_param("Server");
            infmt.set_server(server);
            std::string database = leave->get_op().get_param("Database");
            std::string collection = leave->get_op().get_param("Collection");
            infmt.set_ns(database, collection);
            std::string username = leave->get_op().get_param("Username");
            std::string password = leave->get_op().get_param("Password");
            if (!username.empty())
                infmt.set_auth(username, password);
            husky::load(infmt, [&](std::string& chunk) { workerdriver_info.py_connector->send_string(chunk); });
#endif
        } else {
            // husky::LineInputFormat<husky::LocalFileSplitter> infmt;
            // std::string path = leave->get_op().get_param("Path");
            // infmt.set_input(path);
            // Context::get_worker<BaseWorker>().load(infmt, [&](boost::string_ref& chunk) {
            //     workerdriver_info.py_connector->send_string(chunk.to_string());
            // });
        }
        workerdriver_info.py_connector->send_string("");
    }
    // visit_dag(opdag);
    // listen_from_python(job_util);
    workerdriver_info.py_connector->listen_from_python();
    LOG_I << "worker_run: end instr";
    return 0;
}

}  // namespace husky
