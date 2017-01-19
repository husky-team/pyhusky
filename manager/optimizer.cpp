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

#include "optimizer.hpp"

#include <vector>


namespace husky {

OpDAG Optimizer::concat_pushdown(const OpDAG& input_dag) {
    std::function<void(std::shared_ptr<OpNode>)> remove_concat;
    std::vector<int> concat_nodes;
    remove_concat = [&remove_concat, &concat_nodes](std::shared_ptr<OpNode> node)->void {
        for (auto dep : node->deps) {
            remove_concat(dep);
        }
        std::vector<std::shared_ptr<OpNode>> new_deps;
        for (auto dep : node->get_deps()) {
            if (dep->op.get_op_name() == "concat" || dep->op.get_op_name() == "Functional#concat") {
                concat_nodes.push_back(dep->id);
                for (auto depdep : dep->get_deps()) {
                    // push concat params up
                    auto & params = depdep->get_op().get_params();
                    for (auto & kv : dep->get_op().get_params()) {
                        if (params.find(kv.first) == params.end()) {
                            params[kv.first] = kv.second;
                        }
                    }
                    new_deps.push_back(depdep);
                }
            } else {
                new_deps.push_back(dep);
            }
        }
        node->get_deps() = new_deps;
    };
    OpDAG output_dag = input_dag.deepcopy();
    for (auto node : output_dag.get_leaves()) {
        remove_concat(node);
    }
    for (auto id : concat_nodes) {
        if (output_dag.op_nodes.find(id) != output_dag.op_nodes.end())
            output_dag.op_nodes.erase(id);
    }
    // std::cout << "optimize" << std::endl;
    // output_dag.print();
    return output_dag;
}

OpDAG Optimizer::optimize(const OpDAG& input_dag) {
    return concat_pushdown(input_dag);
}

}  // namespace husky
