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

#include <functional>
#include <list>
#include <string>
#include <vector>

#include "core/context.hpp"
#include "core/zmq_helpers.hpp"

namespace husky {

class ITCWorker;

class PendingData {
  private:
    void* _data;
    std::function<void(void*)> _final;
  public:
    PendingData(void* data, std::function<void(void*)> finalizer);
    PendingData(PendingData&& d);
    PendingData(const PendingData& a) = delete;
    void* data();
    ~PendingData();
};

/**
 * if sending int or long, it is better to directly send them
 */
class ITCDaemon {
  private:
    friend class ITCWorker;  // we are friends~
    static const char* daemon_addr;
    static const char* worker_addr;
    std::vector<ITCWorker*> wbox;
    zmq::socket_t _send, _recv;
    static zmq::context_t& get_zmq_context();
  public:
    ITCDaemon();
    ITCWorker& new_itc_worker(int worker_id);
    /**
     * To construct object in the return statement to avoid using copy constructor
     * data is transferred by move constructor, the pointer can be deleted safely
     */
    std::string recv_string();
    BinStream recv_binstream();
    void broadcast(const std::string& s);
    void broadcast(const BinStream& b);
    ~ITCDaemon();
};

class ITCWorker {
    friend class ITCDaemon;
    zmq::socket_t _send, _recv;
    std::list<PendingData> content;
    int64_t id;
    explicit ITCWorker(int wid);
    ITCWorker(const ITCWorker& box) = delete;
public:
    /**
     * Send a quota to daemon saying I have a thing sent to you
     */
    void sendmore(std::string&& str);
    void send(std::string&& str, int flag = 0);
    void send(BinStream&& bin, int flag = 0);
    std::string recv_string();
    BinStream recv_binstream();
    ~ITCWorker();
};

}  // namespace husky
