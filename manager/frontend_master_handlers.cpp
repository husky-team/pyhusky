#include "frontend_master_handlers.hpp"

namespace husky {

FrontendMasterHandlers::FrontendMasterHandlers() {
    pyhusky_master_handlers.parent = this;
}

int FrontendMasterHandlers::dag_split(const OpDAG & dag, int task_id) {
    int num_jobs = 0;
    for (const auto & i : dag.get_leaves())
        num_jobs += OperationSplitter::op_split(i, pending_jobs, task_id); 
    return num_jobs;
}

void FrontendMasterHandlers::init_master() {
    pyhusky_master_handlers.init_master();
}

}
