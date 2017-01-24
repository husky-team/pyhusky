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

#include "husky/base/serialization.hpp"

namespace husky {

class PythonConnector;
class ITCWorker;
class ITCDaemon;
class PythonSocket;
class ThreadConnector;
class Operation;

using base::BinStream;

class PyHuskySVM {
   public:
    static void init_py_handlers();
    static void init_cpp_handlers();
    static void init_daemon_handlers();

   protected:
    // thread handlers
    static void SVM_load_pyhlist_handler(PythonSocket& python_socket, ITCWorker& daemon_socket);

    // cpp handlers
    static void SVM_init_handler(const Operation& op, PythonSocket& python_socket, ITCWorker& daemon_socket);
    static void SVM_load_hdfs_handler(const Operation& op, PythonSocket& python_socket, ITCWorker& daemon_socket);
    static void SVM_train_handler(const Operation& op, PythonSocket& python_socket, ITCWorker& daemon_socket);

    // daemon handlers
    static void daemon_train_handler(ITCDaemon&, BinStream&);
};  // class PyHuskyML

}  // namespace husky
