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

#include "backend/pythonconnector.hpp"

#include <string>

#include "husky/base/log.hpp"
#include "husky/core/zmq_helpers.hpp"

#include "backend/register.hpp"
#include "manager/itc.hpp"

namespace husky {

// PythonSocket
void PythonSocket::init(int wid) {
    std::string master_port = std::to_string(Context::get_config().get_master_port());
    std::string proc_id = std::to_string(Context::get_worker_info().get_process_id());

    pipe_to_python = new zmq::socket_t(*(Context::get_zmq_context()), ZMQ_PUSH);
    pipe_to_python->connect(
        ("ipc://pyhusky-session-" + master_port + "-proc-" + proc_id + "-" + std::to_string(wid)).c_str());

    pipe_from_python = new zmq::socket_t(*(Context::get_zmq_context()), ZMQ_PULL);
    pipe_from_python->bind(
        ("ipc://cpphusky-session-" + master_port + "-proc-" + proc_id + "-" + std::to_string(wid)).c_str());
}

void PythonSocket::close() {
    delete pipe_to_python;
    delete pipe_from_python;
}

// PythonConnector
std::unordered_map<std::string, std::function<void(PythonSocket& python_socket, ITCWorker& daemon_socket)>>
    PythonConnector::handler_map;

PythonConnector::PythonConnector(ITCWorker& d_util, int wid) : daemon_socket(d_util) { python_socket.init(wid); }
PythonConnector::~PythonConnector() { python_socket.close(); }

void PythonConnector::register_handler() {
    // init all the handlers
    husky::RegisterFunction::register_py_handlers();
}

void PythonConnector::add_handler(const std::string& name,
                                  std::function<void(PythonSocket& python_socket, ITCWorker& daemon_socket)> handler) {
    assert(handler_map.find(name) == handler_map.end() && "handler exists");
    handler_map[name] = handler;
}
void PythonConnector::listen_from_python() {
    // Assumption: only one output!
    while (true) {
        std::string instr = zmq_recv_string(python_socket.pipe_from_python);
        if (instr == "instr_end") {
            daemon_socket.send("instr_end");
            break;
        }
        if (handler_map.find(instr) != handler_map.end()) {
            handler_map[instr](this->python_socket, this->daemon_socket);
        } else {
            throw std::runtime_error("Weird message received in PythonConnector: " + instr);
        }
    }
}

void PythonConnector::start_python_proc(int lid, int gid, int num_workers) {
    std::string master_port = std::to_string(Context::get_config().get_master_port());
    std::string proc_id = std::to_string(Context::get_worker_info().get_process_id());
    extern_py_proc =
        popen(("python -m backend.python.python_backend " + std::to_string(lid) + " " + std::to_string(gid) + " " +
               proc_id + " " + std::to_string(num_workers) + " " + master_port + " > /tmp/log-pyhusky-" + master_port +
               "-proc-" + proc_id + "-" + std::to_string(lid))
                  .c_str(),
              "r");
    // python profile
    // extern_py_proc = popen(("python -m cProfile -o /tmp/prof-"+master_port+"-"+std::to_string(lid)+"
    // backend/python/python_backend.py " + std::to_string(lid)
    //                        + " " + std::to_string(gid) + " " + proc_id + " " + std::to_string(num_workers) + " "
    //                        + master_port + " > /tmp/log-pyhusky-" + master_port + "-proc-" + proc_id + "-" +
    //                        std::to_string(lid)).c_str() , "r");
    // N2N
    auto& worker_info = husky::Context::get_worker_info();
    send_string(std::to_string(husky::Context::get_config().get_comm_port()));
    send_string(std::to_string(worker_info.get_num_processes()));
    for (int i = worker_info.get_num_processes(); i--;) {
        send_string(worker_info.get_hostname(i));
        send_string(std::to_string(worker_info.get_num_local_workers(i)));
    }
    // N2N
}

// BP
std::string PythonConnector::recv_string() { return zmq_recv_string(python_socket.pipe_from_python); }
// BP

void PythonConnector::close_python_proc() { pclose(extern_py_proc); }

void PythonConnector::send_string(const std::string& data) { zmq_send_string(python_socket.pipe_to_python, data); }
void PythonConnector::send_binstream(BinStream& data) { zmq_send_binstream(python_socket.pipe_to_python, data); }
}  // namespace husky
