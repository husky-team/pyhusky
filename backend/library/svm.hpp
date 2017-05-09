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

#include "husky/lib/ml/data_loader.hpp"
#include "husky/lib/ml/feature_label.hpp"
#include "husky/lib/ml/parameter.hpp"
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
    static void SVM_test_handler(const Operation& op, PythonSocket& python_socket, ITCWorker& daemon_socket);

    // daemon handlers
    static void daemon_train_handler(ITCDaemon&, BinStream&);

    template <bool is_sparse>
    static void train_SVM(const Operation& op, PythonSocket& python_socket, ITCWorker& daemon_socket);
    template <bool is_sparse>
    static void test_SVM(const Operation& op, PythonSocket& python_socket, ITCWorker& daemon_socket);
    template <bool is_sparse>
    static void create_model_from_pyhuskylist(std::string name, PythonSocket& python_socket, ITCWorker& daemon_socket);
};  // class PyHuskyML

class SVMModel {
   public:
    SVMModel(int num_features)
      : num_features(num_features) {
    }

    husky::lib::ml::ParameterBucket<double>& init_param_list() {
        if (param_list) {
            delete param_list;
        }
        param_list = new husky::lib::ml::ParameterBucket<double>(num_features + 1);
        return *param_list;
    }

    husky::lib::ml::ParameterBucket<double>& get_param_list() {
        return *param_list;
    }

    ~SVMModel() {
        if (param_list) {
            delete param_list;
        }
    }

    int num_features;
    husky::lib::ml::ParameterBucket<double>* param_list = nullptr;
};

extern thread_local std::unordered_map<std::string, std::shared_ptr<SVMModel>> SVM_models;

template <bool is_sparse>
void SVM_create_model_from_url(std::string name, std::string url, husky::lib::ml::DataFormat data_format) {
    husky::base::log_msg("create model name: " + name);

    using LabeledPointHObj = husky::lib::ml::LabeledPointHObj<double, double, is_sparse>;
    auto& load_list = husky::ObjListStore::create_objlist<LabeledPointHObj>(name);

    // load data
    int num_features = husky::lib::ml::load_data(url, load_list, data_format);

    // init model
    assert(num_features > 0);
    SVM_models[name] = std::make_shared<SVMModel>(num_features);
}

}  // namespace husky
