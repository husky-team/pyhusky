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
#include "manager/itc.hpp"
#include "husky/base/serialization.hpp"

namespace husky {

using base::BinStream;
/// \brief DaemonHandler uses ThreadConnector to communicate with threads
class ThreadConnector {
public:
    ThreadConnector();
    void start();
    void close();
    ITCWorker & new_itc_worker(int);
    ITCDaemon & get_itc_connector();
    void broadcast_to_threads(std::string& instr);
    void broadcast_to_threads(BinStream& binstream);
    void listen_from_threads(BinStream& buffer);

    /// register handlers to handle messages from thread 
    static void add_handler(const std::string&,
            std::function<void(ITCDaemon&, BinStream&)>);

    /// Must Invoke at the beginning
    static void register_handler();

private:
    ITCDaemon *to_worker;
    static std::unordered_map<std::string, 
        std::function<void(ITCDaemon&, BinStream&)>> handler_map;
};

}  // namespace husky
