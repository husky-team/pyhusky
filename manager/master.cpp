#include "husky/master/master.hpp"

#include <string>
#include <vector>

#include "husky/core/job_runner.hpp"
#include "frontend_master_handlers.hpp"

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("serve");

// #ifdef WITH_HDFS
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
// #endif

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
