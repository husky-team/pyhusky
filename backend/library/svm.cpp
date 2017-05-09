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

#include "backend/library/svm.hpp"

#include <algorithm>
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

using husky::lib::ml::ParameterBucket;

using husky::lib::Aggregator;
using husky::lib::AggregatorFactory;

thread_local std::unordered_map<std::string, std::shared_ptr<SVMModel>> SVM_models;

// how to get label and feature from data object

void PyHuskySVM::init_py_handlers() {
    PythonConnector::add_handler("SVMModel#SVM_load_pyhlist_py", SVM_load_pyhlist_handler);
}

void PyHuskySVM::init_cpp_handlers() {
    WorkerDriver::add_handler("SVMModel#SVM_init_py", SVM_init_handler);
    WorkerDriver::add_handler("SVMModel#SVM_load_hdfs_py", SVM_load_hdfs_handler);
    WorkerDriver::add_handler("SVMModel#SVM_train_py", SVM_train_handler);
    WorkerDriver::add_handler("SVMModel#SVM_test_py", SVM_test_handler);
}

void PyHuskySVM::init_daemon_handlers() {
    ThreadConnector::add_handler("SVMModel#SVM_train", daemon_train_handler);
    ThreadConnector::add_handler("SVMModel#SVM_test", daemon_train_handler);
}

void PyHuskySVM::SVM_load_pyhlist_handler(PythonSocket& python_socket, ITCWorker& daemon_socket) {
    LOG_I << "start SVM_load_pyhlist";
    // override
    std::string name = zmq_recv_string(python_socket.pipe_from_python);
    std::string sparse = zmq_recv_string(python_socket.pipe_from_python);

    // create model
    if (sparse == "true") {
        PyHuskySVM::create_model_from_pyhuskylist<true>(name, python_socket, daemon_socket);
    } else {
        PyHuskySVM::create_model_from_pyhuskylist<false>(name, python_socket, daemon_socket);
    }

    LOG_I << "finish SVM_load_pyhlist";
}

template <bool is_sparse>
void PyHuskySVM::create_model_from_pyhuskylist(std::string name, PythonSocket& python_socket, ITCWorker& daemon_socket) {
    using LabeledPointHObj = husky::lib::ml::LabeledPointHObj<double, double, is_sparse>;
    auto& load_list = husky::ObjListStore::create_objlist<LabeledPointHObj>(name);

    int n_sample = std::stoi(zmq_recv_string(python_socket.pipe_from_python));

    husky::lib::Aggregator<int> n_feature_agg(0, [](int& a, const int& b) { a = std::max(a, b); });
    auto& ac = husky::lib::AggregatorFactory::get_channel();

    int num_features = 0;

    for (int i = 0; i < n_sample; i++) {
        int n = std::stoi(zmq_recv_string(python_socket.pipe_from_python));
        LabeledPointHObj this_obj(n);
        for (int j = 0; j < n; j++) {
            int X_idx = std::stoi(zmq_recv_string(python_socket.pipe_from_python));
            double X_elem = std::stod(zmq_recv_string(python_socket.pipe_from_python));
            this_obj.x.set(X_idx, X_elem);
            num_features = std::max(num_features, X_idx + 1);
        }
        double y = std::stod(zmq_recv_string(python_socket.pipe_from_python));
        this_obj.y = y;
        load_list.add_object(this_obj);
    }

    n_feature_agg.update(num_features);
    husky::lib::AggregatorFactory::sync();
    num_features = n_feature_agg.get_value();

    list_execute(load_list, [&](LabeledPointHObj& this_obj) {
        if (this_obj.x.get_feature_num() != num_features) {
            this_obj.x.resize(num_features);
        }
    });

    assert(num_features > 0);
    SVM_models[name] = std::make_shared<SVMModel>(num_features);
}

void PyHuskySVM::SVM_init_handler(const Operation& op, PythonSocket& python_socket, ITCWorker& daemon_socket) {
    LOG_I << "SVM_init_handler";
}

void PyHuskySVM::SVM_load_hdfs_handler(const Operation& op, PythonSocket& python_socket, ITCWorker& daemon_socket) {
    LOG_I << "SVM_load_hdfs_handler";

    const std::string& url = op.get_param("url");
    const std::string& name = op.get_param("list_name");
    const std::string& format = op.get_param("format");

    // load data
    const std::string& sparse = op.get_param("is_sparse");
    bool is_sparse = sparse == "true" ? true : false;
    husky::lib::ml::DataFormat data_format = 
        format == "tsv" ? husky::lib::ml::kTSVFormat : husky::lib::ml::kLIBSVMFormat;
    if (is_sparse) {
        SVM_create_model_from_url<true>(name, url, data_format);
    } else {
        SVM_create_model_from_url<false>(name, url, data_format);
    }
    LOG_I << "create SVM Model"; 
}

template <bool is_sparse>
void PyHuskySVM::train_SVM(const Operation& op, PythonSocket& python_socket, ITCWorker& daemon_socket) {
    LOG_I << "start SVM_train";
    // Get Parameters sent from python
    const std::string& name = op.get_param("list_name");
    using LabeledPointHObj = husky::lib::ml::LabeledPointHObj<double, double, is_sparse>;
    auto& load_list = husky::ObjListStore::get_objlist<LabeledPointHObj>(name);

    // get model config parameters
    double lambda = std::stod(op.get_param("lambda"));
    int num_iter = std::stoi(op.get_param("n_iter"));

    // initialize parameters
    ParameterBucket<double>& param_list = SVM_models[name]->init_param_list();  // scalar b and vector w
    int num_features = SVM_models[name]->num_features;

    if (husky::Context::get_global_tid() == 0) {
        LOG_I << "num of params: " + std::to_string(param_list.get_num_param());
        LOG_I << "num of features: " << num_features;
    }
    // get the number of global records
    Aggregator<int> num_samples_agg(0, [](int& a, const int& b) { a += b; });
    num_samples_agg.update(load_list.get_size());
    AggregatorFactory::sync();
    int num_samples = num_samples_agg.get_value();
    if (husky::Context::get_global_tid() == 0) {
        LOG_I << "Training set size = " + std::to_string(num_samples);
    }

    // Aggregators for regulator, w square and loss
    Aggregator<double> sqr_w_agg(0.0, [](double& a, const double& b) { a += b; });
    sqr_w_agg.to_reset_each_iter();
    Aggregator<double> loss_agg(0.0, [](double& a, const double& b) { a += b; });
    loss_agg.to_reset_each_iter();

    // Main loop
    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < num_iter; i++) {
        double sqr_w = 0.0;      // ||w||^2
        double regulator = std::stod(op.get_param_or("C", "0"));  // prevent overfitting

        // calculate w square
        for (int idx = 0; idx < num_features; idx++) {
            double w = param_list.param_at(idx);
            sqr_w += w * w;
        }

        // get local copy of parameters
        husky::lib::Vector<double, false> bweight = param_list.get_all_param(); // bweight, 0 - n-1: weights, n: intercept

        // calculate regulator
        regulator = regulator ? regulator : (sqr_w == 0) ? 1.0 : std::min(1.0, 1.0 / sqrt(sqr_w * lambda));
        if (regulator < 1) {
            bweight *= regulator;
            sqr_w = 1 / lambda;
        }

        double eta = 1.0 / (i + 1);

        // regularize w in param_list
        if (husky::Context::get_global_tid() == 0) {
            for (int idx = 0; idx < num_features ; idx++) {
                double w = bweight[idx];
                param_list.update(idx, (w - w / regulator - eta * w));
            }
        }

        auto& ac = AggregatorFactory::get_channel();
        // calculate gradient
        husky::list_execute(load_list, {}, {&ac}, [&](LabeledPointHObj& this_obj) {
            double prod = 0;  // prod = WX * y
            double y = this_obj.y;
            auto X = this_obj.x;
            for (auto it = X.begin_feaval(); it != X.end_feaval(); ++it)
                prod += bweight[(*it).fea] * (*it).val;
            // bias
            prod += bweight[num_features];
            prod *= y;

            if (prod < 1) {  // the data point falls within the margin
                for (auto it = X.begin_feaval(); it != X.end_feaval(); ++it) {
                    auto x = *it;
                    x.val *= y;  // calculate the gradient for each parameter
                    param_list.update(x.fea, eta * x.val / num_samples / lambda);
                }
                // update bias
                param_list.update(num_features, eta * y / num_samples);
                loss_agg.update(1 - prod);
            }
            sqr_w_agg.update(sqr_w);
        });

        sqr_w = sqr_w_agg.get_value() / num_samples;
        double loss = lambda / 2 * sqr_w + loss_agg.get_value() / num_samples;
        if (husky::Context::get_global_tid() == 0) {
            LOG_I << "Iteration " + std::to_string(i + 1) + ": ||w|| = " + std::to_string(sqrt(sqr_w)) + ", loss = " +
                         std::to_string(loss);
        }
    }
    auto end = std::chrono::steady_clock::now();

    // Show result
    if (husky::Context::get_global_tid() == 0) {
        // param_list.present();
        LOG_I << "Time per iter: " +
             std::to_string(std::chrono::duration_cast<std::chrono::duration<float>>(end - start).count() / num_iter);
        LOG_I << "send back the parameter to pyhusky";
        BinStream result;
        result << param_list.get_num_param();
        for (auto v : param_list.get_all_param()) {
            result << v;
        }
        daemon_socket.sendmore("SVMModel#SVM_train");
        daemon_socket.send(std::move(result));
    }

    LOG_I << "finish SVM_finish";
}

void PyHuskySVM::SVM_train_handler(const Operation& op, PythonSocket& python_socket, ITCWorker& daemon_socket) {
    const std::string& sparse = op.get_param("is_sparse");
    bool is_sparse = sparse == "true" ? true : false;
    if (is_sparse) {
        PyHuskySVM::train_SVM<true>(op, python_socket, daemon_socket);
    } else {
        PyHuskySVM::train_SVM<false>(op, python_socket, daemon_socket);
    }
}

template<bool is_sparse>
void PyHuskySVM::test_SVM(const Operation& op, PythonSocket& python_socket, ITCWorker& daemon_socket) {
    using LabeledPointHObj = husky::lib::ml::LabeledPointHObj<double, double, is_sparse>;
    const std::string& format = op.get_param("format");
    husky::lib::ml::DataFormat data_format = 
        format == "tsv" ? husky::lib::ml::kTSVFormat : husky::lib::ml::kLIBSVMFormat;
    const std::string& name = op.get_param("list_name");

	auto& test_set = husky::ObjListStore::create_objlist<LabeledPointHObj>();
	husky::lib::ml::load_data(op.get_param("url"), test_set, data_format);

    ParameterBucket<double>& param_list = SVM_models[name]->get_param_list();  // scalar b and vector w
    int num_features = SVM_models[name]->num_features;

    Aggregator<int> accu_agg(0, [](int& a, const int& b) { a += b; });
    Aggregator<int> num_test_agg(0, [](int& a, const int& b) { a += b; });
    auto& ac = AggregatorFactory::get_channel();
    auto bweight = param_list.get_all_param();
    list_execute(test_set, {}, {&ac}, [&](LabeledPointHObj& this_obj) {
        double indicator = 0;
        auto y = this_obj.y;
        auto X = this_obj.x;
        for (auto it = X.begin_feaval(); it != X.end_feaval(); it++)
            indicator += bweight[(*it).fea] * (*it).val;
        // bias
        indicator += bweight[num_features];
        indicator *= y;  // right prediction if positive (Wx+b and y have the same sign)
        if (indicator >= 0)
            accu_agg.update(1);
        num_test_agg.update(1);
    });

    if (husky::Context::get_global_tid() == 0) {
		BinStream result;
		double accu = static_cast<double>(accu_agg.get_value()) / num_test_agg.get_value();
		result << accu;
        husky::LOG_I << "Accuracy rate on testing set: " << accu;
		daemon_socket.sendmore("SVMModel#SVM_test");
		daemon_socket.send(std::move(result));
    }
}


void PyHuskySVM::SVM_test_handler(const Operation& op, PythonSocket& python_socket, ITCWorker& daemon_socket) {
    const std::string& sparse = op.get_param("is_sparse");
    bool is_sparse = sparse == "true" ? true : false;
    if (is_sparse) {
        PyHuskySVM::test_SVM<true>(op, python_socket, daemon_socket);
    } else {
        PyHuskySVM::test_SVM<false>(op, python_socket, daemon_socket);
    }
}

void PyHuskySVM::daemon_train_handler(ITCDaemon& to_worker, BinStream& buffer) {
    BinStream recv = to_worker.recv_binstream();
    int flag = 1;  // 1 means sent by cpp
    buffer << flag << recv.to_string();
}

}  // namespace husky
