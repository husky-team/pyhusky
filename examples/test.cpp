// Copyright 2015 Husky Team
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

#include <random>
#include <vector>

#include "core/engine.hpp"
#include "io/input/hdfs_line_inputformat.hpp"
#include "base/log.hpp"

class Line {
    public:
        typedef std::string KeyT;
        Line(const KeyT& _key):key(_key){}
        Line() = default;
        KeyT key;
    
        virtual KeyT const & id() const {return key;}
        friend husky::BinStream & operator>>(husky::BinStream & in, Line & r) {
            return in >> r.key;
        }
        friend husky::BinStream & operator<<(husky::BinStream & out, const Line & r) {
            return out << r.key;
        }
};

void test() {
    husky::io::HDFSLineInputFormat infmt;
    infmt.set_input(husky::Context::get_param("input"));

    husky::ObjList<Line> line_list;
    auto& ch = husky::ChannelFactory::create_push_channel<std::string>(line_list, line_list);

    int i = 0;
    husky::load(infmt, [&](boost::string_ref& chunk) {
        i = i + 1;
        line_list.add_object(Line(chunk.to_string()));
    });

    husky::base::log_msg("^^^num: " + std::to_string(i));

    husky::base::log_msg("*****workerdriver globalize1111...");

    husky::globalize(line_list);
    
    husky::base::log_msg("*****workerdriver globalize2222...");
}

int main(int argc, char ** argv) {
    std::vector<std::string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("input");
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(test);
        return 0;
    }
    return 1;
}
