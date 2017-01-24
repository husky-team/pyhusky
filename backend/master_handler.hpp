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

#include <list>
#include <string>
#include <utility>

#include "husky/master/master.hpp"

namespace husky {

class FrontendMasterHandlers;

class PyHuskyMasterHandlers {
    friend FrontendMasterHandlers;

   public:
    void init_master();

    void session_begin_handler();

    void session_end_handler();

    void new_task_handler();

    void query_task_handler();

    void task_end_handler();

    void request_instruction_handler();

   protected:
    int need_session_begin_py = 0;
    int need_session_end_py = 0;
    // int cur_generation = 0;
    bool py_started = false;
    // std::vector<int> daemon_generation;
    std::list<std::pair<int, std::string>> task_results;  // {int,sting}:{from python/cpp, content}
    FrontendMasterHandlers* parent;
};

}  // namespace husky
