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

#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <string>

#include "husky/base/log.hpp"
#include "husky/base/serialization.hpp"
#include "husky/core/engine.hpp"
#include "husky/lib/ml/data_loader.hpp"
#include "husky/lib/ml/fgd.hpp"
#include "husky/lib/ml/logistic_regression.hpp"

#include "backend/itc.hpp"
#include "backend/python_connector.hpp"

namespace husky {

class PythonConnector;
class ITCWorker;
class ITCDaemon;
class PythonSocket;
class ThreadConnector;
class Operation;

using base::BinStream;

using husky::lib::ml::ParameterBucket;

class PyHuskyGraph {
   public:
    static void init_py_handlers();
    static void init_cpp_handlers();
    static void init_daemon_handlers();

   protected:
    // python_handlers
    static void load_edgelist_phlist_handler(PythonSocket & python_socket,
            ITCWorker & daemon_socket);  // to handle load_edgelist_phlist

    // cpp_handlers
#ifdef WITH_HDFS
    static void load_adjlist_hdfs_handler(const Operation & op, PythonSocket & python_socket,
            ITCWorker & daemon_socket);  // to handle load_adjlist_hdfs_py
#endif
    static void pagerank_handler(const Operation & op, PythonSocket & python_socket,
            ITCWorker & daemon_socket);  // to handle pagerank
    static void pagerank_topk_handler(const Operation& op, PythonSocket & python_socket,
            ITCWorker & daemon_socket);  // to handle pagerank_topk

    // daemon handlers
    static void daemon_topk_handler(ITCDaemon&, BinStream&);
};

}  // namespace husky
