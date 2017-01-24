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

#include <cstdio>
#include <functional>
#include <string>
#include <unordered_map>

#include "husky/base/serialization.hpp"
#include "husky/core/zmq_helpers.hpp"

namespace husky {

/// \brief PythonSocket is used for PythonConnector to communicate with python process,
/// including two zmq sockets and init() and close() function
struct PythonSocket {
    zmq::socket_t* pipe_to_python;
    zmq::socket_t* pipe_from_python;

    void init(int wid);
    void close();
};

using base::BinStream;

class ITCWorker;
/// \brief WorkerDriver uses PythonConnector to communicate with python process.
///
/// 1, start(), close(): start and close python process
///
/// 2, register_handler(): register handlers to handle messages from python
///
/// 3, listen_from_python(): main function for receiving messages from python
class PythonConnector {
   public:
    friend class WorkerDriver;
    PythonConnector(ITCWorker& d_util, int wid);
    ~PythonConnector();

    /// main function for receiving messages from python
    void listen_from_python();

    /// start a python process
    void start_python_proc(int lid, int gid, int num_workers);

    /// close a python process
    void close_python_proc();

    void send_string(const std::string& data);
    void send_binstream(BinStream& data);

    // BP
    std::string recv_string();
    // BP

    /// register handlers to handle messages from python
    static void add_handler(const std::string& name,
                            std::function<void(PythonSocket& python_socket, ITCWorker& daemon_socket)> handler);

    /// Must invoke at the beginning
    static void register_handler();

   private:
    PythonSocket python_socket;
    // const DaemonUtil& daemon_socket;
    ITCWorker& daemon_socket;
    FILE* extern_py_proc;

    static std::unordered_map<std::string, std::function<void(PythonSocket& python_socket, ITCWorker& daemon_socket)>>
        handler_map;
};

}  // namespace husky
