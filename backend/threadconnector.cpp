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

#include "backend/threadconnector.hpp"

#include "husky/base/log.hpp"
#include "husky/base/serialization.hpp"
#include "husky/core/context.hpp"
#include "husky/core/zmq_helpers.hpp"

#include "backend/register.hpp"

namespace husky {

std::unordered_map<std::string, std::function<void(ITCDaemon&, BinStream&)>> ThreadConnector::handler_map;

ThreadConnector::ThreadConnector() {}

void ThreadConnector::register_handler() {
    // Register all the handlers
    husky::RegisterFunction::register_daemon_handlers();
}

void ThreadConnector::add_handler(const std::string& name, std::function<void(ITCDaemon&, BinStream&)> handler) {
    assert(handler_map.find(name) == handler_map.end() && "handler exists");
    handler_map[name] = handler;
}

void ThreadConnector::start() { to_worker = new ITCDaemon; }

void ThreadConnector::close() {
    delete to_worker;
    to_worker = nullptr;
}

ITCWorker& ThreadConnector::new_itc_worker(int id) { return to_worker->new_itc_worker(id); }

ITCDaemon& ThreadConnector::get_itc_connector() { return *to_worker; }

void ThreadConnector::broadcast_to_threads(std::string& instr) {
    // for (int i=0; i<passers.size(); ++i) {
    //     zmq_send_string(passers[i], instr);
    // }
    to_worker->broadcast(instr);
}
void ThreadConnector::broadcast_to_threads(BinStream& binstream) {
    // for (int i=0; i<passers.size(); ++i) {
    //     zmq_send_binstream(passers[i], binstream);
    // }
    to_worker->broadcast(binstream);
}
void ThreadConnector::listen_from_threads(BinStream& buffer) {
    int num_local_workers = Context::get_worker_info().get_num_local_workers();
    int num_finished_workers = 0;
    while (true) {
        // std::string instr = zmq_recv_string(this->receiver);
        std::string instr = to_worker->recv_string();
        if (instr == "instr_end") {
            ++num_finished_workers;
            if (num_finished_workers == num_local_workers)
                break;
        } else {
            if (handler_map.find(instr) != handler_map.end()) {
                handler_map[instr](*(this->to_worker), buffer);
            } else {
                throw std::runtime_error("Weird message received in ThreadConnector: " + instr);
            }
        }
    }
}
}  // namespace husky
