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
#include <thread>
#include <unordered_map>
#include <vector>

#include "boost/thread.hpp"

#include "husky/base/serialization.hpp"
#include "husky/core/mailbox.hpp"

#include "backend/threadconnector.hpp"

namespace husky {

using base::BinStream;

/// \brief DaemonInfo stores information needed by daemon
struct DaemonInfo {
    explicit DaemonInfo(std::string _config_file) : config_file(_config_file) {}

    std::vector<boost::thread*> threads;
    // std::vector<LocalMailbox*> mailboxes;
    ThreadConnector thread_connector;
    std::string instr;
    std::string config_file;
    BinStream dag_bin;
    CentralRecver* recver;
    MailboxEventLoop* el;
    LocalMailbox* mailbox;
    std::vector<LocalMailbox*> mailboxes;
    int init_param_ac;
    char** init_param_av;
    std::vector<std::string> init_param_args;
};

/// \brief DaemonDriver runs the daemon
class DaemonDriver {
   public:
    static bool init_with_args(int ac, char** av, const std::vector<std::string>& customized);
    static void daemon_run(int argc, char** argv, std::vector<std::string> args);
    static void job(const std::string& config_file);
    static void init_daemon_handler_map();
    static void start_workers(DaemonInfo&);
    static void add_new_handler(const std::string&, std::function<void(DaemonInfo&)>);
    // operators
    static void session_begin(DaemonInfo& daemon_info);
    static void session_end(DaemonInfo& daemon_info);
    static void new_instr_py(DaemonInfo& daemon_info);

   private:
    static std::unordered_map<std::string, std::function<void(DaemonInfo&)>> daemon_handler_map;
};

}  // namespace husky
