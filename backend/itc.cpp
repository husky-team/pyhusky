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

#include "backend/itc.hpp"

#include <string>

#include "husky/base/log.hpp"

namespace husky {

#ifdef ITCTEST
zmq::context_t& ITCDaemon::get_zmq_context() {
    static zmq::context_t ctx;
    return ctx;
}
#else
zmq::context_t& ITCDaemon::get_zmq_context() { return *(Context::get_zmq_context()); }
#endif

PendingData::PendingData(void* data, std::function<void(void*)> finalizer) : _data(data), _final(finalizer) {}

PendingData::PendingData(PendingData&& d) : _data(d._data), _final(d._final) {
    d._data = nullptr;
    d._final = nullptr;
}

void* PendingData::data() { return _data; }

PendingData::~PendingData() {
    if (_data) {
        _final(_data);
    }
}

/**
* if sending int or long, it is better to directly send them
*/
const char* ITCDaemon::daemon_addr = "inproc://husky-daemon";
const char* ITCDaemon::worker_addr = "inproc://husky-worker";

ITCDaemon::ITCDaemon() : _recv(get_zmq_context(), ZMQ_PULL), _send(get_zmq_context(), ZMQ_PUB) {
    std::string master_port = Context::get_param("master_port");
    _recv.bind(std::string(daemon_addr) + master_port);
    _send.bind(std::string(worker_addr) + master_port);
}
ITCWorker& ITCDaemon::new_itc_worker(int worker_id) {
    if (wbox.size() <= worker_id)
        wbox.resize(worker_id + 1);
    return *(wbox[worker_id] = new ITCWorker(worker_id));
}
/**
 * To construct object in the return statement to avoid using copy constructor
 * data is transferred by move constructor, the pointer can be deleted safely
 */
std::string ITCDaemon::recv_string() {
    int wid = zmq_recv_int32(&_recv);
    auto& list = wbox[wid]->content;
    list.pop_front();
    auto& str = list.front();
    return std::move(*(std::string*) str.data());
}

BinStream ITCDaemon::recv_binstream() {
    int wid = zmq_recv_int32(&_recv);
    auto& list = wbox[wid]->content;
    list.pop_front();
    auto& bin = list.front();
    return std::move(*static_cast<BinStream*>(bin.data()));
}

void ITCDaemon::broadcast(const std::string& s) { zmq_send_string(&_send, s); }

void ITCDaemon::broadcast(const BinStream& b) { zmq_send_binstream(&_send, b); }

ITCDaemon::~ITCDaemon() {
    for (auto& i : wbox)
        if (i)
            delete i;
    _send.close();
    _recv.close();
}

ITCWorker::ITCWorker(int wid)
    : id(wid), _send(ITCDaemon::get_zmq_context(), ZMQ_PUSH), _recv(ITCDaemon::get_zmq_context(), ZMQ_SUB) {
    std::string master_port = Context::get_param("master_port");
    _send.connect(std::string(ITCDaemon::daemon_addr) + master_port);
    _recv.connect(std::string(ITCDaemon::worker_addr) + master_port);
    // Note: setsockopt(int, void*, size_t) the third arg is to indicate the len of the second arg, here len("") is 0
    _recv.setsockopt(ZMQ_SUBSCRIBE, "", 0);  // receive all message from daemon, all messages will pass the filter
    // _recv.setsockopt(ZMQ_SUBSCRIBE, std::to_string(wid), 1);//might add this in the future?
    content.push_back(PendingData(nullptr, nullptr));
}

void ITCWorker::sendmore(std::string&& str) { send(std::move(str), ZMQ_SNDMORE); }
/**
 * Send a quota to daemon saying I have a thing sent to you
 */
void ITCWorker::send(std::string&& str, int flag) {
    content.push_back(PendingData(static_cast<void*>(new std::string(std::move(str))),
                                  [](void* p) { delete static_cast<std::string*>(p); }));
    zmq_send_int32(&_send, id, flag);
}

void ITCWorker::send(BinStream&& bin, int flag) {
    content.push_back(PendingData(static_cast<void*>(new BinStream(std::move(bin))),
                                  [](void* p) { delete static_cast<BinStream*>(p); }));
    zmq_send_int32(&_send, id, flag);
}

std::string ITCWorker::recv_string() { return zmq_recv_string(&_recv); }

BinStream ITCWorker::recv_binstream() { return zmq_recv_binstream(&_recv); }

ITCWorker::~ITCWorker() {
    _send.close();
    _recv.close();
}

}  // namespace husky
