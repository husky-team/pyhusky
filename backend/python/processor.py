# Copyright 2016 Husky Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import json
import struct
import cloudpickle
import cPickle
from frontend.binstream import BinStream
from backend.python.globalvar import GlobalVar, GlobalSocket

def log_msg(msg):
    print msg
    sys.stdout.flush()

def get_map_partition(node):
    map_partitions = []
    if node.op_name == "Functional#map_partition_py":
        map_partitions.append(node)
    for dep in node.op_deps:
        map_partitions += get_map_partition(dep)
    return map_partitions

def preprocess_instr(node):
    func = GlobalVar.name_to_prefunc.get(node.op_name)
    if func is not None:
        func(node)
    for dep in node.op_deps:
        preprocess_instr(dep)

def postprocess_instr(node):
    func = GlobalVar.name_to_postfunc.get(node.op_name)
    if func is not None:
        func(node)
    for dep in node.op_deps:
        postprocess_instr(dep)

def visit(op, data):
    if GlobalVar.name_to_type[op.op_name] == "transformation":
        new_data = GlobalVar.name_to_func[op.op_name](op, data)
        for dep in op.op_deps:
            visit(dep, new_data)
    elif GlobalVar.name_to_type[op.op_name] == "action" or GlobalVar.name_to_type[op.op_name] == "library":
        GlobalVar.name_to_func[op.op_name](op, data)

def finish_instr():
    GlobalSocket.pipe_to_cpp.send("instr_end")

def handle_instr(instr_id):
    instr = GlobalSocket.pipe_from_cpp.recv()
    if instr == "session_end_py":
        return True
    elif instr == "new_instr_py":
        bin_dag = BinStream()
        bin_dag.data_buf = GlobalSocket.pipe_from_cpp.recv()
        root = bin_dag.load_dag()
        
        log_msg("The root of this instrution is: "+ root.op_name)
        preprocess_instr(root)
        map_partitions = get_map_partition(root)
        if root.op_name in ["Functional#load_py", "Functional#load_cache_py", \
                "Functional#reduce_by_key_end_py", "Functional#group_by_key_end_py", "Functional#distinct_end_py", \
                "Functional#parallelize_py", "Functional#difference_end_py"] :
            load_generator = GlobalVar.name_to_func[root.op_name](root)
            for chunk in load_generator:
                for dep in root.op_deps:
                    visit(dep, chunk)

        for map_partition in map_partitions:  # Jump to that map_partition and continue
            chunk = GlobalVar.name_to_func["Functional#load_map_partition_py"](map_partition)
            for dep in map_partition.op_deps:
                visit(dep, chunk)

        postprocess_instr(root)
        finish_instr()
        return False
