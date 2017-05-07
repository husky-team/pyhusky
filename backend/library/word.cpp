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

#include "backend/library/word.hpp"

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

class Word {
   public:
    using KeyT = std::string;

    Word() = default;
    explicit Word(const KeyT& w) : word(w) {}
    const KeyT& id() const { return word; }

    KeyT word;
    int count = 0;
};

void PyHuskyWord::init_py_handlers() {
    PythonConnector::add_handler("Word#load_phlist_py", to_words_handler);
}

void PyHuskyWord::init_cpp_handlers() {
#ifdef WITH_HDFS
    WorkerDriver::add_handler("Word#load_hdfs_py", load_hdfs_handler);
#endif
    WorkerDriver::add_handler("Word#wordcount_py", wordcount_handler);
    WorkerDriver::add_handler("Word#wordcount_topk_py", wordcount_topk_handler);
    WorkerDriver::add_handler("Word#wordcount_print_py", wordcount_print_handler);
    WorkerDriver::add_handler("Word#del_py", del_handler);
}

void PyHuskyWord::init_daemon_handlers() {
    ThreadConnector::add_handler("Word#wordcount_topk", daemon_wordcount_topk_handler);
    ThreadConnector::add_handler("Word#wordcount_print", daemon_wordcount_print_handler);
}

void PyHuskyWord::del_handler(const Operation & op, PythonSocket & python_socket, ITCWorker & daemon_socket) {
}

#ifdef WITH_HDFS
void PyHuskyWord::load_hdfs_handler(const Operation & op, PythonSocket & python_socket, ITCWorker & daemon_socket) {
    // Get words from hdfs
    std::string name = op.get_param("list_name");
    std::string url = op.get_param("url");
    auto& word_list = husky::ObjListStore::create_objlist<Word>(name);
    auto& infmt = io::InputFormatStore::create_line_inputformat();
    infmt.set_input(url);
    auto& ch = husky::ChannelStore::create_push_combined_channel<int, husky::SumCombiner<int>>(infmt, word_list, name);
    auto parse_wc = [&](boost::string_ref& chunk) {
        if (chunk.size() == 0)
            return;
        boost::char_separator<char> sep(" \t");
        boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);
        for (auto& w : tok) {
            ch.push(1, w);
        }
    };
    load(infmt, {&ch}, parse_wc);
}
#endif

void PyHuskyWord::to_words_handler(PythonSocket & python_socket, ITCWorker & daemon_socket) {
    // Get words from Python
    std::string name = zmq_recv_string(python_socket.pipe_from_python);
    int num = std::stoi(zmq_recv_string(python_socket.pipe_from_python));
    // std::cout << "name: " << name << std::endl;
    auto& word_list = husky::ObjListStore::create_objlist<Word>(name);
    auto& ch = husky::ChannelStore::create_push_combined_channel<int, husky::SumCombiner<int>>(word_list, word_list, name);
    for (int i = 0; i < num; ++i) {
        std::string word;
        word = zmq_recv_string(python_socket.pipe_from_python);
        // std::cout << "word: " << word << std::endl;
        ch.push(1, word);
    }
    ch.out();
}

void PyHuskyWord::wordcount_handler(const Operation & op, PythonSocket & python_socket, ITCWorker & daemon_wocket) {
    // Calculate wordcount
    std::string name = op.get_param("list_name");
    // std::cout << "list_name: " << name << std::endl;
    auto& ch = husky::ChannelStoreBase::get_push_combined_channel<int, husky::SumCombiner<int>, Word>(name);
    auto& word_list = husky::ObjListStore::get_objlist<Word>(name);
    list_execute(word_list, {&ch}, {}, [&](Word& word){
        word.count = ch.get(word);
        // std::cout << "Inside list: " << word.word << " " << word.count << std::endl;
    });
}

void PyHuskyWord::wordcount_print_handler(const Operation & op, PythonSocket & python_socket, ITCWorker & daemon_socket) {
    // Send back all the wordcound
    std::string name = op.get_param("list_name");
    auto& word_list = husky::ObjListStore::get_objlist<Word>(name);
    BinStream result;
    int k = word_list.get_size();
    result << k;
    list_execute(word_list, {}, {}, [&](Word& word){
        result << word.word << word.count;
    });
    daemon_socket.sendmore("Word#wordcount_print");
    daemon_socket.send(std::move(result));
}

void PyHuskyWord::wordcount_topk_handler(const Operation & op, PythonSocket & python_socket, ITCWorker & daemon_socket) {
    // Use aggregator to get the topk, and send back the topk
    std::string name = op.get_param("list_name");
    int kMaxNum = std::stoi(op.get_param("k"));
    auto& word_list = husky::ObjListStore::get_objlist<Word>(name);
    typedef std::set<std::pair<int, std::string>> TopKPairs;
    auto add_to_topk = [kMaxNum](TopKPairs& pairs, const std::pair<int, std::string>& p) {
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
                add_to_topk(pairs, husky::base::deser<std::pair<int, std::string>>(in));
        },
        [](husky::base::BinStream& out, const TopKPairs& pairs) {
            out << pairs.size();
            for (auto& p : pairs)
                out << p;
        });
    list_execute(word_list, {}, {}, [&](Word& word){
        unique_topk.update(add_to_topk, std::make_pair(word.count, word.id()));
    });
    husky::lib::AggregatorFactory::sync();
    if (husky::Context::get_global_tid() == 0) {
        BinStream result;
        result << static_cast<int>(unique_topk.get_value().size());
        for (auto& i : unique_topk.get_value()) {
            // std::cout << "topk: " << i.second << " " << i.first << std::endl;
            result << i.second << i.first;
        }
        daemon_socket.sendmore("Word#wordcount_topk");
        daemon_socket.send(std::move(result));
    }
}

void PyHuskyWord::daemon_wordcount_topk_handler(ITCDaemon & to_worker, BinStream & buffer) {
    BinStream recv = to_worker.recv_binstream();
    int flag = 1;  // 1 means sent by cpp
    buffer << flag << recv.to_string();
}
void PyHuskyWord::daemon_wordcount_print_handler(ITCDaemon & to_worker, BinStream & buffer) {
    BinStream recv = to_worker.recv_binstream();
    int flag = 1;  // 1 means sent by cpp
    buffer << flag << recv.to_string();
}

}  // namespace husky
