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

#include "backend/library/functional.hpp"

#include <string>
#include <vector>

#include "husky/base/log.hpp"
#include "husky/core/context.hpp"
#include "husky/core/engine.hpp"
#include "husky/core/zmq_helpers.hpp"
#include "husky/io/hdfs_manager.hpp"

#include "backend/pythonconnector.hpp"
#include "backend/threadconnector.hpp"
#include "manager/itc.hpp"

namespace husky {

void PyHuskyFunctional::init_py_handlers() {
    PythonConnector::add_handler("Functional#reduce_end", reduce_end_handler);
    PythonConnector::add_handler("Functional#count_end", count_end_handler);
    PythonConnector::add_handler("Functional#collect_end", collect_end_handler);
    PythonConnector::add_handler("Functional#reduce_by_key", reduce_by_key_handler);
    PythonConnector::add_handler("Functional#reduce_by_key_end", reduce_by_key_end_handler);

    PythonConnector::add_handler("Functional#group_by_key", reduce_by_key_handler);
    PythonConnector::add_handler("Functional#group_by_key_end", reduce_by_key_end_handler);
    PythonConnector::add_handler("Functional#distinct", distinct_handler);
    PythonConnector::add_handler("Functional#distinct_end", distinct_end_handler);
    PythonConnector::add_handler("Functional#difference", difference_handler);
    PythonConnector::add_handler("Functional#difference_end", difference_end_handler);
#ifdef WITH_HDFS
    PythonConnector::add_handler("Functional#write_to_hdfs", write_to_hdfs_handler);
#endif
}

void PyHuskyFunctional::init_daemon_handlers() {
    ThreadConnector::add_handler("Functional#reduce_end", daemon_functional_end_handler);
    ThreadConnector::add_handler("Functional#count_end", daemon_functional_end_handler);
    ThreadConnector::add_handler("Functional#collect_end", daemon_functional_end_handler);
}

void PyHuskyFunctional::reduce_end_handler(PythonSocket& python_socket, ITCWorker& daemon_socket) {
    std::string value = zmq_recv_string(python_socket.pipe_from_python);
    // create objlist
    husky::ObjList<GeneralObject> reduce_list;
    // create ch
    auto& reduce_ch = husky::ChannelStore::create_push_channel<std::string>(reduce_list, reduce_list);

    reduce_ch.push(value, 0);
    reduce_ch.flush();

    list_execute(reduce_list, [&](GeneralObject& r) {
        auto& msgs = reduce_ch.get(r);
        for (auto& msg : msgs) {
            zmq_send_string(python_socket.pipe_to_python, msg);
        }
    });

    zmq_send_string(python_socket.pipe_to_python, "");

    // receive the reduce result
    value = zmq_recv_string(python_socket.pipe_from_python);
    if (Context::get_worker_info().get_process_id() == 0) {
        LOG_I << "get result, sending to daemon";
        daemon_socket.sendmore("Functional#count_end");
        daemon_socket.send(std::move(value));
    }
}
void PyHuskyFunctional::count_end_handler(PythonSocket& python_socket, ITCWorker& daemon_socket) {
    int value = std::stoi(zmq_recv_string(python_socket.pipe_from_python));
    // create objlist
    husky::ObjList<GeneralObject> count_list;
    // create ch
    auto& count_ch =
        husky::ChannelStore::create_push_combined_channel<int, husky::SumCombiner<int>>(count_list, count_list);

    count_ch.push(value, 0);
    count_ch.flush();

    int result = -1;
    list_execute(count_list, [&](GeneralObject& r) {
        auto& msg = count_ch.get(r);
        result = msg;
    });

    zmq_send_string(python_socket.pipe_to_python, "");

    if (result != -1) {
        LOG_I << "get result, sending to daemon";
        daemon_socket.sendmore("Functional#count_end");
        // force to dump to avoid to send it to pythonbackend
        daemon_socket.send(std::move("I" + std::to_string(result)) + "\n.");
    }
}

void PyHuskyFunctional::collect_end_handler(PythonSocket& python_socket, ITCWorker& daemon_socket) {
    std::string name = zmq_recv_string(python_socket.pipe_from_python);
    std::string content = zmq_recv_string(python_socket.pipe_from_python);
    daemon_socket.sendmore("Functional#collect_end");
    daemon_socket.send(std::move(content));
}

void PyHuskyFunctional::distinct_handler(PythonSocket& python_socket, ITCWorker& daemon_socket) {
    // receive name
    std::string name = zmq_recv_string(python_socket.pipe_from_python);
    // create objlist
    auto& distinct_list = ObjListStore::create_objlist<ReduceObject>(name);
    // create channel
    auto& distinct_ch = husky::ChannelStore::create_push_combined_channel<std::string, husky::SumCombiner<std::string>>(
        distinct_list, distinct_list, name);
    // receive the num of key-value pairs
    int num = std::stoi(zmq_recv_string(python_socket.pipe_from_python));
    for (int i = 0; i < num; i++) {
        std::string key = zmq_recv_string(python_socket.pipe_from_python);
        std::string value;
        distinct_ch.push(value, key);
    }
}
void PyHuskyFunctional::distinct_end_handler(PythonSocket& python_socket, ITCWorker& daemon_socket) {
    // receive name
    std::string name = zmq_recv_string(python_socket.pipe_from_python);
    // get channel
    auto& distinct_end_ch =
        ChannelStoreBase::get_push_combined_channel<std::string, SumCombiner<std::string>, ReduceObject>(name);
    // get objlist
    auto& distinct_end_list = ObjListStore::get_objlist<ReduceObject>(name);

    // flush
    distinct_end_ch.flush();

    list_execute(distinct_end_list, [&](ReduceObject& r) { zmq_send_string(python_socket.pipe_to_python, r.key); });

    zmq_send_string(python_socket.pipe_to_python, "");

    ChannelStoreBase::drop_channel(name);
    ObjListStore::drop_objlist(name);
}
void PyHuskyFunctional::difference_handler(PythonSocket& python_socket, ITCWorker& daemon_socket) {
    // receive name
    std::string name = zmq_recv_string(python_socket.pipe_from_python);
    std::string diff_type = zmq_recv_string(python_socket.pipe_from_python);
    int type;
    if (diff_type == "left") {
        type = 0;
    } else {
        type = 1;
    }

    // create objlist
    auto& diff_list = ObjListStore::has_objlist(name) ? ObjListStore::get_objlist<ReduceObject>(name)
                                                      : ObjListStore::create_objlist<ReduceObject>(name);

    // create channel
    auto& diff_ch = husky::ChannelStore::has_channel(name)
                        ? ChannelStoreBase::get_push_combined_channel<int, SumCombiner<int>, ReduceObject>(name)
                        : husky::ChannelStore::create_push_combined_channel<int, husky::SumCombiner<int>>(
                              diff_list, diff_list, name);

    // receive the num of key-value pairs
    int num = std::stoi(zmq_recv_string(python_socket.pipe_from_python));
    for (int i = 0; i < num; i++) {
        std::string key = zmq_recv_string(python_socket.pipe_from_python);
        diff_ch.push(type, key);
    }
}
void PyHuskyFunctional::difference_end_handler(PythonSocket& python_socket, ITCWorker& daemon_socket) {
    // receive name
    std::string name = zmq_recv_string(python_socket.pipe_from_python);
    // get channel
    auto& diff_end_ch = ChannelStoreBase::get_push_combined_channel<int, SumCombiner<int>, ReduceObject>(name);
    // get objlist
    auto& diff_end_list = ObjListStore::get_objlist<ReduceObject>(name);

    // flush
    diff_end_ch.flush();

    list_execute(diff_end_list, {&diff_end_ch}, {}, [&](ReduceObject& r) {
        auto msg = diff_end_ch.get(r);
        if (msg == 0) {
            zmq_send_string(python_socket.pipe_to_python, r.key);
            zmq_send_string(python_socket.pipe_to_python, std::to_string(msg));
        }
    });

    zmq_send_string(python_socket.pipe_to_python, "");

    ChannelStoreBase::drop_channel(name);
    ObjListStore::drop_objlist(name);
}

void PyHuskyFunctional::reduce_by_key_handler(PythonSocket& python_socket, ITCWorker& daemon_socket) {
    // receive name
    std::string name = zmq_recv_string(python_socket.pipe_from_python);
    // create objlist
    auto& reduce_list = ObjListStore::create_objlist<ReduceObject>(name);
    // create channel
    auto& reduce_ch = husky::ChannelStore::create_push_channel<std::string>(reduce_list, reduce_list, name);
    // receive the num of key-value pairs
    int num = std::stoi(zmq_recv_string(python_socket.pipe_from_python));
    for (int i = 0; i < num; i++) {
        std::string key = zmq_recv_string(python_socket.pipe_from_python);
        std::string value = zmq_recv_string(python_socket.pipe_from_python);
        reduce_ch.push(value, key);
    }
}

void PyHuskyFunctional::reduce_by_key_end_handler(PythonSocket& python_socket, ITCWorker& daemon_socket) {
    // receive name
    std::string name = zmq_recv_string(python_socket.pipe_from_python);
    // get channel
    auto& reduce_end_ch = husky::ChannelStore::get_push_channel<std::string, ReduceObject>(name);
    // get objlist
    auto& reduce_end_list = ObjListStore::get_objlist<ReduceObject>(name);

    // flush
    reduce_end_ch.flush();

    list_execute(reduce_end_list, [&](ReduceObject& r) {
        auto& msgs = reduce_end_ch.get(r);
        zmq_send_string(python_socket.pipe_to_python, r.key);
        zmq_send_string(python_socket.pipe_to_python, std::to_string(msgs.size()));
        for (auto& msg : msgs) {
            zmq_send_string(python_socket.pipe_to_python, msg);
        }
    });

    zmq_send_string(python_socket.pipe_to_python, "");

    ChannelStoreBase::drop_channel(name);
    ObjListStore::drop_objlist(name);
}

#ifdef WITH_HDFS
void PyHuskyFunctional::write_to_hdfs_handler(PythonSocket& python_socket, ITCWorker& daemon_socket) {
    std::string hdfs_host = Context::get_param("hdfs_namenode");
    std::string hdfs_port = Context::get_param("hdfs_namenode_port");
    std::string name = zmq_recv_string(python_socket.pipe_from_python);
    std::string url = zmq_recv_string(python_socket.pipe_from_python);
    int num = std::stoi(zmq_recv_string(python_socket.pipe_from_python));
    for (int i = 0; i < num; ++i) {
        std::string content = zmq_recv_string(python_socket.pipe_from_python);
        io::HDFS::Write(hdfs_host, hdfs_port, content, url, Context::get_worker_info().get_process_id());
    }
    // Need to close_all_files since Daemon is not terminated.
    io::HDFS::CloseFile(hdfs_host, hdfs_port);
}
#endif

void PyHuskyFunctional::daemon_functional_end_handler(ITCDaemon& to_worker, BinStream& buffer) {
    std::string recv = to_worker.recv_string();
    int flag = 0;  // 0 means sent by python
    buffer << flag << recv;
    LOG_I << "received from job_listener";
}

}  // namespace husky
