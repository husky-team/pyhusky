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

import json
import cPickle
from python.pyhusky.backend.globalvar import GlobalVar, GlobalSocket, OperationParam

def register_all():
    # register graph
    FromEdgelist.register()

class FromEdgelist:
    @staticmethod
    def register():
        GlobalVar.name_to_func["Graph#load_edgelist_phlist_py"] = FromEdgelist.func
        GlobalVar.name_to_prefunc["Graph#load_edgelist_phlist_py"] = FromEdgelist.prefunc
        GlobalVar.name_to_func["Graph#load_edgelist_phlist_end_py"] = None
        GlobalVar.name_to_postfunc["Graph#load_edgelist_phlist_end_py"] = FromEdgelist.end_postfunc

        GlobalVar.name_to_type["Graph#load_edgelist_phlist_py"] = GlobalVar.librarytype
        GlobalVar.name_to_type["Graph#load_edgelist_phlist_end_py"] = GlobalVar.librarytype 
    @staticmethod
    def func(op, data):
        for x in data:
            assert type(x) is tuple and len(x) is 2
            GlobalVar.edgelist_store.append((str(x[0]), str(x[1])))
    @staticmethod
    def prefunc(op):
        GlobalVar.edgelist_list = op.op_param[OperationParam.list_str]
        GlobalVar.edgelist_store = []
    @staticmethod
    def end_postfunc(op):
        # send out edgelist_store
        GlobalSocket.send("Graph#load_edgelist_phlist_py")
        GlobalSocket.send(GlobalVar.edgelist_list)
        GlobalSocket.send(str(len(GlobalVar.edgelist_store)))
        for (x,y) in GlobalVar.edgelist_store:
            GlobalSocket.send(x)
            GlobalSocket.send(y)
        GlobalVar.edgelist_store = []
