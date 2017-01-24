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

#include "backend/register.hpp"

#include "backend/library/functional.hpp"
#include "backend/library/linear_regression.hpp"
#include "backend/library/logistic_regression.hpp"

namespace husky {

void RegisterFunction::register_py_handlers() {
    PyHuskyFunctional::init_py_handlers();
    PyHuskyLinearR::init_py_handlers();
    // PyHuskySVM::init_py_handlers();
    PyHuskyLogisticR::init_py_handlers();
}

void RegisterFunction::register_cpp_handlers() {
    PyHuskyLinearR::init_cpp_handlers();
    // PyHuskySVM::init_cpp_handlers();
    PyHuskyLogisticR::init_cpp_handlers();
}

void RegisterFunction::register_daemon_handlers() {
    PyHuskyFunctional::init_daemon_handlers();
    PyHuskyLinearR::init_daemon_handlers();
    // PyHuskySVM::init_daemon_handlers();
    PyHuskyLogisticR::init_daemon_handlers();
}

}  // namespace husky
