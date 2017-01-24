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
#include "husky/lib/ml/data_loader.hpp"
#include "husky/lib/ml/feature_label.hpp"
#include "husky/lib/ml/parameter.hpp"
#include "husky/lib/ml/vector_linalg.hpp"

#include "backend/pythonconnector.hpp"
#include "backend/threadconnector.hpp"
#include "backend/workerdriver.hpp"
#include "manager/itc.hpp"
#include "manager/operation.hpp"

namespace husky {

using husky::lib::ml::SparseFeatureLabel;
using husky::lib::ml::ParameterBucket;

using husky::lib::Aggregator;
using husky::lib::AggregatorFactory;

typedef SparseFeatureLabel ObjT;

// how to get label and feature from data object
double get_y_(ObjT& this_obj) { return this_obj.get_label(); }
std::vector<std::pair<int, double>> get_X_(ObjT& this_obj) { return this_obj.get_feature(); }

void PyHuskySVM::init_py_handlers() {
    PythonConnector::add_handler("SVMModel#SVM_load_pyhlist_py", SVM_load_pyhlist_handler);
}

void PyHuskySVM::init_cpp_handlers() {
    WorkerDriver::add_handler("SVMModel#SVM_init_py", SVM_init_handler);
    WorkerDriver::add_handler("SVMModel#SVM_load_hdfs_py", SVM_load_hdfs_handler);
    WorkerDriver::add_handler("SVMModel#SVM_train_py", SVM_train_handler);
}

void PyHuskySVM::init_daemon_handlers() { ThreadConnector::add_handler("SVMModel#SVM_train", daemon_train_handler); }

void PyHuskySVM::SVM_load_pyhlist_handler(PythonSocket& python_socket, ITCWorker& daemon_socket) {
    LOG_I << "start SVM_load_pyhlist";

    LOG_I << "set_model";

    LOG_I << "finish SVM_load_pyhlist";
}

void PyHuskySVM::SVM_init_handler(const Operation& op, PythonSocket& python_socket, ITCWorker& daemon_socket) {
    LOG_I << "SVM_init_handler";
}

void PyHuskySVM::SVM_load_hdfs_handler(const Operation& op, PythonSocket& python_socket, ITCWorker& daemon_socket) {
    LOG_I << "SVM_load_hdfs_handler";
    // overide
    // Get Parameters sent from python
    const std::string& url = op.get_param("url");
    const std::string& name = op.get_param("list_name");
    auto& load_list = husky::ObjListFactory::create_objlist<SparseFeatureLabel>(name);

    // load data
    husky::lib::ml::DataLoader<SparseFeatureLabel> data_loader(husky::lib::ml::kLIBSVMFormat);
    data_loader.load_info(url, load_list);
    int num_features = data_loader.get_num_feature();

    // get model config parameters
    double lambda = std::stod(husky::Context::get_param("lambda"));
    int num_iter = std::stoi(husky::Context::get_param("n_iter"));

    // initialize parameters
    ParameterBucket<double> param_list(num_features + 1);  // scalar b and vector w

    if (husky::Context::get_global_tid() == 0) {
        LOG_I << "num of params: " + std::to_string(param_list.get_num_param());
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
    Aggregator<double> regulator_agg(0.0, [](double& a, const double& b) { a += b; });
    Aggregator<double> sqr_w_agg(0.0, [](double& a, const double& b) { a += b; });
    sqr_w_agg.to_reset_each_iter();
    Aggregator<double> loss_agg(0.0, [](double& a, const double& b) { a += b; });
    loss_agg.to_reset_each_iter();

    // Main loop
    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < num_iter; i++) {
        double sqr_w = 0.0;      // ||w||^2
        double regulator = 0.0;  // prevent overfitting

        // calculate w square
        for (int idx = 1; idx <= num_features; idx++) {
            double w = param_list.param_at(idx);
            sqr_w += w * w;
        }

        // get local copy of parameters
        std::vector<double> bweight = param_list.get_all_param();

        // calculate regulator
        regulator = (sqr_w == 0) ? 1.0 : std::min(1.0, 1.0 / sqrt(sqr_w * lambda));
        if (regulator < 1) {
            bweight *= regulator;
            sqr_w = 1 / lambda;
        }

        double eta = 1.0 / (i + 1);

        // regularize w in param_list
        if (husky::Context::get_global_tid() == 0) {
            for (int idx = 1; idx < bweight.size(); idx++) {
                double w = bweight[idx];
                param_list.update(idx, (w - w / regulator - eta * w));
            }
        }

        auto& ac = AggregatorFactory::get_channel();
        // calculate gradient
        husky::list_execute(load_list, {}, {&ac}, [&](ObjT& this_obj) {
            double prod = 0;  // prod = WX * y
            double y = get_y_(this_obj);
            std::vector<std::pair<int, double>> X = get_X_(this_obj);
            for (auto& x : X)
                prod += bweight[x.first] * x.second;
            // bias
            prod += bweight[0];
            prod *= y;

            if (prod < 1) {  // the data point falls within the margin
                for (auto& x : X) {
                    x.second *= y;  // calculate the gradient for each parameter
                    param_list.update(x.first, eta * x.second / num_samples / lambda);
                }
                // update bias
                param_list.update(0, eta * y / num_samples);
                loss_agg.update(1 - prod);
            }
            sqr_w_agg.update(sqr_w);
            regulator_agg.update(regulator);
        });

        int num_samples = num_samples_agg.get_value();
        sqr_w = sqr_w_agg.get_value() / num_samples;
        regulator = regulator_agg.get_value() / num_samples;
        double loss = lambda / 2 * sqr_w + loss_agg.get_value() / num_samples;
        if (husky::Context::get_global_tid() == 0) {
            LOG_I << "Iteration " + std::to_string(i + 1) + ": ||w|| = " + std::to_string(sqrt(sqr_w)) + ", loss = " +
                         std::to_string(loss);
        }
    }
    auto end = std::chrono::steady_clock::now();

    // Show result
    if (husky::Context::get_global_tid() == 0) {
        param_list.present();
        LOG_I << "Time per iter: " +
                     std::to_string(std::chrono::duration_cast<std::chrono::duration<float>>(end - start).count() /
                                    num_iter);
    }
}

void PyHuskySVM::SVM_train_handler(const Operation& op, PythonSocket& python_socket, ITCWorker& daemon_socket) {
    // override
    LOG_I << "start SVM_train";

    LOG_I << "finish SVM_finish";
}

void PyHuskySVM::daemon_train_handler(ITCDaemon& to_worker, BinStream& buffer) {
    BinStream recv = to_worker.recv_binstream();
    int flag = 1;  // 1 means sent by cpp
    buffer << flag << recv.to_string();
}

}  // namespace husky
