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

#include "backend/library/linear_regression.hpp"

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "backend/pythonconnector.hpp"
#include "backend/threadconnector.hpp"
#include "backend/workerdriver.hpp"
#include "manager/itc.hpp"
#include "manager/operation.hpp"
#include "core/context.hpp"
#include "core/utils.hpp"
#include "core/zmq_helpers.hpp"
#include "base/log.hpp"

#include "core/engine.hpp"
#include "lib/ml/data_loader.hpp"
#include "lib/ml/linear_regression.hpp"
#include "lib/ml/scaler.hpp"
#include "lib/ml/sgd.hpp"

namespace husky {

typedef std::vector<double> vec_double;

using husky::lib::ml::ParameterBucket;

thread_local std::map<std::string, std::shared_ptr<ModelBase>> local_SGD_LinearR_model;

void PyHuskyLinearR::init_py_handlers() {
    PythonConnector::add_handler("LinearRegressionModel#LinearR_load_pyhlist_py", LinearR_load_pyhlist_handler);
}

void PyHuskyLinearR::init_cpp_handlers() {
    WorkerDriver::add_handler("LinearRegressionModel#LinearR_init_py", LinearR_init_handler);
    WorkerDriver::add_handler("LinearRegressionModel#LinearR_load_hdfs_py", LinearR_load_hdfs_handler);
    WorkerDriver::add_handler("LinearRegressionModel#LinearR_train_py", LinearR_train_handler);
}

void PyHuskyLinearR::init_daemon_handlers() {
    ThreadConnector::add_handler("LinearRegressionModel#LinearR_train", daemon_train_handler);
}

void PyHuskyLinearR::LinearR_load_pyhlist_handler(PythonSocket & python_socket, ITCWorker & daemon_socket) {
    LOG_I << "start LinearR_load_pyhlist";
    //override
    std::string name = zmq_recv_string(python_socket.pipe_from_python);
    std::string sparse = zmq_recv_string(python_socket.pipe_from_python);

    // create model
    if (sparse == "1") {Linear_create_model_from_pyhuskylist<true>(name, python_socket, daemon_socket);}
    else {Linear_create_model_from_pyhuskylist<false>(name, python_socket, daemon_socket);}

    LOG_I << "create SGD Linear Regression Model";
}

void PyHuskyLinearR::LinearR_init_handler(const Operation & op,
        PythonSocket & python_socket,
        ITCWorker & daemon_socket) {
    LOG_I << "LinearR_init_handler";
}

void PyHuskyLinearR::LinearR_load_hdfs_handler(const Operation & op,
        PythonSocket & python_socket,
        ITCWorker & daemon_socket) {
    // overide
    // Get Parameters sent from python
    const std::string & url = op.get_param("url");
    const std::string & sparse = op.get_param("is_sparse");
    const std::string & format = op.get_param("format");

    // is_sparse
    bool is_sparse = sparse == "1" ? true : false;
    husky::lib::ml::DataFormat data_format = format == "tsv" ? husky::lib::ml::kTSVFormat : husky::lib::ml::kLIBSVMFormat;
    const std::string& name = op.get_param("list_name");

    // create model
    if (is_sparse) {Linear_create_model_from_url<true>(name, url, data_format);}
    else {Linear_create_model_from_url<false>(name, url, data_format);}
    
    LOG_I << "create SGD Linear Regression Model";
}

void PyHuskyLinearR::LinearR_train_handler(const Operation & op,
        PythonSocket & python_socket,
        ITCWorker & daemon_socket) {
    // override
    LOG_I << "start LinearR_train";
    
    const std::string & sparse = op.get_param("is_sparse");
    bool is_sparse = sparse == "1" ? true : false;
    const std::string & name = op.get_param("list_name");
    double alpha = std::stod(op.get_param("alpha"));
    int num_iter = std::stoi(op.get_param("n_iter"));

    // train model
    if (is_sparse) {Linear_train_model<true>(name, alpha, num_iter);}
    else {Linear_train_model<false>(name, alpha, num_iter);}

    // Send Back the parameter to pyHusky
    if (husky::Context::get_global_tid() == 0) {
        LOG_I << "send back the parameter to pyHusky";
        BinStream result = is_sparse ? Linear_get_params<true>(name): Linear_get_params<false>(name);
        daemon_socket.sendmore("LinearRegressionModel#LinearR_train");
        daemon_socket.send(std::move(result));
    }
    LOG_I << "finish LinearR_finish";
}

void PyHuskyLinearR::daemon_train_handler(ITCDaemon & to_worker, BinStream & buffer) {
    BinStream recv = to_worker.recv_binstream();
    int flag = 1;  // 1 means sent by cpp
    buffer << flag << recv.to_string();
}

}  // End of namespace Husky
