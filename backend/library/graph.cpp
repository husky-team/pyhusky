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

#include "backend/library/graph.hpp"

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

class Vertex {
   public:
    using KeyT = int;
    Vertex() : pr(0.25) {}
    explicit Vertex(const KeyT& id) : key(id), pr(0.15) {}
    const KeyT& id() const { return key; }

    KeyT key;
    std::vector<KeyT> nbs;
    float pr;
};

void PyHuskyGraph::init_py_handlers() {
    PythonConnector::add_handler("Graph#load_edgelist_phlist_py", load_edgelist_phlist_handler);
}

void PyHuskyGraph::init_cpp_handlers() {
#ifdef WITH_HDFS
    WorkerDriver::add_handler("Graph#load_adjlist_hdfs_py", load_adjlist_hdfs_handler);
#endif
    WorkerDriver::add_handler("Graph#pagerank_py", pagerank_handler);
    WorkerDriver::add_handler("Graph#pagerank_topk_py", pagerank_topk_handler);
}

void PyHuskyGraph::init_daemon_handlers() {
    ThreadConnector::add_handler("Graph#topk_cpp", daemon_topk_handler);
}


#ifdef WITH_HDFS
void PyHuskyGraph::load_adjlist_hdfs_handler(const Operation & op, PythonSocket & python_socket, ITCWorker & daemon_socket) {
}
#endif

void PyHuskyGraph::load_edgelist_phlist_handler(PythonSocket & python_socket, ITCWorker & daemon_socket) {
    // Get edges from Python
    std::string name = zmq_recv_string(python_socket.pipe_from_python);
    int num = std::stoi(zmq_recv_string(python_socket.pipe_from_python));
    auto& vertex_list = husky::ObjListStore::create_objlist<Vertex>(name);
    auto& ch = husky::ChannelStore::create_push_channel<int>(vertex_list, vertex_list, name);
    for (int i = 0; i < num; ++i) {
        int src = std::stoi(zmq_recv_string(python_socket.pipe_from_python));
        int dst = std::stoi(zmq_recv_string(python_socket.pipe_from_python));
        ch.push(dst, src);
    }
    ch.out();
}

void PyHuskyGraph::pagerank_handler(const Operation & op, PythonSocket & python_socket, ITCWorker & daemon_socket) {
    // Calculate pagerank
    // 1. Construct graph from edgelist
    std::string name = op.get_param("list_name");
    int num_iter = stoi(op.get_param("iter"));
    auto& ch = husky::ChannelStoreBase::get_push_channel<int, Vertex>(name);
    auto& vertex_list = husky::ObjListStore::get_objlist<Vertex>(name);
    list_execute(vertex_list, {&ch}, {}, [&](Vertex& v){
        v.nbs = ch.get(v);
    });
    auto& prch = husky::ChannelStore::create_push_combined_channel<float, husky::SumCombiner<float>>(vertex_list, vertex_list);
    // 2. Calculate PR
    for (int iter = 0; iter < num_iter; ++ iter) {
        list_execute(vertex_list, {&prch}, {&prch}, [&prch, iter](Vertex& u) {
            if (iter > 0)
                u.pr = 0.85 * prch.get(u) + 0.15;

            if (u.nbs.size() == 0)
                return;
            float sendPR = u.pr / u.nbs.size();
            for (auto& nb : u.nbs) {
                prch.push(sendPR, nb);
            }
        });
    }
    /*
    // 3. Show PR results
    list_execute(vertex_list, {}, {}, [&](Vertex& v){
        std::cout << "id: " << v.id() << " pr: " << v.pr << std::endl;
    });
    */
}

void PyHuskyGraph::pagerank_topk_handler(const Operation & op, PythonSocket & python_socket, ITCWorker & daemon_socket) {
    const std::string & name = op.get_param("list_name");
    const int kMaxNum = stoi(op.get_param("k"));
    auto& vertex_list = husky::ObjListStore::get_objlist<Vertex>(name);
    typedef std::set<std::pair<float, int>> TopKPairs;
    auto add_to_topk = [kMaxNum](TopKPairs& pairs, const std::pair<float, int>& p) {
        if (pairs.size() == kMaxNum && *pairs.begin() < p)
            pairs.erase(pairs.begin());
        if (pairs.size() < kMaxNum)
            pairs.insert(p);
    };
    husky::lib::Aggregator<TopKPairs> unique_topk(
        TopKPairs(),
        [add_to_topk](TopKPairs& a, const TopKPairs& b) {
            for (auto& i : b)
                add_to_topk(a, i);
        },
        [](TopKPairs& a) { a.clear(); },
        [add_to_topk](husky::base::BinStream& in, TopKPairs& pairs) {
            pairs.clear();
            for (size_t n = husky::base::deser<size_t>(in); n--;)
                add_to_topk(pairs, husky::base::deser<std::pair<float, int>>(in));
        },
        [](husky::base::BinStream& out, const TopKPairs& pairs) {
            out << pairs.size();
            for (auto& p : pairs)
                out << p;
        });
    list_execute(vertex_list, {}, {}, [&](Vertex& v){
        unique_topk.update(add_to_topk, std::make_pair(v.pr, v.id()));
    });
    husky::lib::AggregatorFactory::sync();
    if (husky::Context::get_global_tid() == 0) {
        BinStream result;
        result << static_cast<int>(unique_topk.get_value().size());
        for (auto& i : unique_topk.get_value()) {
            // std::cout << "topk: " << i.second << " " << i.first << std::endl;
            result << i.second << i.first;
        }
        daemon_socket.sendmore("Graph#topk_cpp");
        daemon_socket.send(std::move(result));
    }
}

void PyHuskyGraph::daemon_topk_handler(ITCDaemon & to_worker, BinStream & buffer) {
    BinStream recv = to_worker.recv_binstream();
    int flag = 1;  // 1 means sent by cpp
    buffer << flag << recv.to_string();
}

}  // namespace husky
