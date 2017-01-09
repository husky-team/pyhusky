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

#include <string>
#include "base/serialization.hpp"
// #include "core/baseobject.hpp"

namespace husky {

using base::BinStream;

class ReduceObject {
public:
    typedef std::string KeyT;
    std::string key;

    explicit ReduceObject(KeyT key) { this->key = key; }

    const KeyT& id() const { return key; }
};

class GeneralObject {
public:
    typedef int KeyT;
    int key;

    explicit GeneralObject(KeyT key) { this->key = key; }
    
    virtual KeyT const & id() const { return key; }
};

class PythonConnector;
class ITCWorker;
class ITCDaemon;
class PythonSocket;
class ThreadConnector;

class PyHuskyFunctional {
public:
    static void init_py_handlers();
    static void init_daemon_handlers();

protected:
    // thread handlers
    static void reduce_end_handler(PythonSocket & python_socket,
            ITCWorker & daemon_socket);  // to handle reduce_end
    static void count_end_handler(PythonSocket & python_socket,
            ITCWorker & daemon_socket);  // to handle count_end
    static void collect_end_handler(PythonSocket & python_socket,
            ITCWorker & daemon_socket);  // to handle collect_end
    static void reduce_by_key_handler(PythonSocket & python_socket,
            ITCWorker & daemon_socket);  // to handle reduce(group)_by_key
    static void reduce_by_key_end_handler(PythonSocket & python_socket,
            ITCWorker & daemon_socket);  // to handle reduce(group)_by_key_end
    static void distinct_handler(PythonSocket & python_socket,
            ITCWorker & daemon_socket);  // to handle distinct
    static void distinct_end_handler(PythonSocket & python_socket,
            ITCWorker & daemon_socket);  // to handle distinct_end
    static void difference_handler(PythonSocket & python_socket,
            ITCWorker & daemon_socket);  // to handle difference
    static void difference_end_handler(PythonSocket & python_socket,
            ITCWorker & daemon_socket);  // to handle difference_end
    static void write_to_hdfs_handler(PythonSocket & python_socket,
            ITCWorker & daemon_socket);  // to handle write_to_hdfs

    // daemon handlers
    static void daemon_functional_end_handler(ITCDaemon&, BinStream&);
};

}  // namespace husky
