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

#include <string>
#include <vector>

#include "husky/core/job_runner.hpp"
#include "husky/master/master.hpp"

#include "master/frontend_master_handlers.hpp"

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("serve");

#ifdef WITH_HDFS
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
#endif

    if (husky::init_with_args(argc, argv, args)) {
        auto& master = husky::Master::get_instance();
        master.setup();

        husky::FrontendMasterHandlers* frontend_master_handlers;
        // add pyhusky handlers
        frontend_master_handlers = new husky::FrontendMasterHandlers();
        frontend_master_handlers->init_master();

        master.serve();
        return 0;
    }
    return 1;
}
