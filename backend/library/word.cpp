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

#include "backend/library/word.hpp"

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "husky/base/log.hpp"
#include "husky/core/context.hpp"
#include "husky/core/engine.hpp"
#include "husky/core/utils.hpp"
#include "husky/core/zmq_helpers.hpp"

#include "backend/itc.hpp"
#include "backend/operation.hpp"
#include "backend/python_connector.hpp"
#include "backend/thread_connector.hpp"
#include "backend/worker_driver.hpp"

namespace husky {

void PyHuskyWord::init_py_handlers() {
    PythonConnector::add_handler("Word#load_phlist_py", to_words_handler);
}

void PyHuskyWord::init_cpp_handlers() {
#ifdef WITH_HDFS
    WorkerDriver::add_handler("Word#load_hdfs_py", load_hdfs_handler);
#endif
    WorkerDriver::add_handler("Word#wordcount_py", wordcount_handler);
    WorkerDriver::add_handler("Word#wordcount_topk_py", wordcount_topk_handler);
    WorkerDriver::add_handler("Word#wordcount_print_py", wordcount_print_handler);
    WorkerDriver::add_handler("Word#del_py", del_handler);
}

void PyHuskyWord::init_daemon_handlers() {
    ThreadConnector::add_handler("Word#wordcount_topk", daemon_wordcount_topk_handler);
    ThreadConnector::add_handler("Word#wordcount_print", daemon_wordcount_print_handler);
}

void PyHuskyWord::del_handler(const Operation & op, PythonSocket & python_socket, ITCWorker & daemon_socket) {
}

#ifdef WITH_HDFS
void PyHuskyWord::load_hdfs_handler(const Operation & op, PythonSocket & python_socket, ITCWorker & daemon_socket) {
}
#endif

void PyHuskyWord::to_words_handler(PythonSocket & python_socket, ITCWorker & daemon_socket) {
    std::string name = zmq_recv_string(python_socket.pipe_from_python);
    int num = std::stoi(zmq_recv_string(python_socket.pipe_from_python));
    for (int i = 0; i < num; ++i) {
        std::string word;
        word = zmq_recv_string(python_socket.pipe_from_python);
        std::cout << "word: " << word << std::endl;
    }
}

void PyHuskyWord::wordcount_handler(const Operation & op, PythonSocket & python_socket, ITCWorker & daemon_wocket) {
}

void PyHuskyWord::wordcount_print_handler(const Operation & op, PythonSocket & python_socket, ITCWorker & daemon_socket) {
}

void PyHuskyWord::wordcount_topk_handler(const Operation & op, PythonSocket & python_socket, ITCWorker & daemon_socket) {
}

void PyHuskyWord::daemon_wordcount_topk_handler(ITCDaemon & to_worker, BinStream & buffer) {
    BinStream recv = to_worker.recv_binstream();
    int flag = 1;  // 1 means sent by cpp
    buffer << flag << recv.to_string();
}
void PyHuskyWord::daemon_wordcount_print_handler(ITCDaemon & to_worker, BinStream & buffer) {
    BinStream recv = to_worker.recv_binstream();
    int flag = 1;  // 1 means sent by cpp
    buffer << flag << recv.to_string();
}

}  // namespace husky
