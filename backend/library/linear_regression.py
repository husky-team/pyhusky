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
from backend.python.globalvar import GlobalVar, GlobalSocket, OperationParam

import sys

def register_all():
    LinearModel.register()

class LinearModel:
    @staticmethod
    def register():
        GlobalVar.name_to_prefunc["LinearRegressionModel#LinearR_load_pyhlist_py"] = LinearModel.load_pyhlist_prefunc
        GlobalVar.name_to_func["LinearRegressionModel#LinearR_load_pyhlist_py"] = LinearModel.load_pyhlist_func
        GlobalVar.name_to_func["LinearRegressionModel#LinearR_load_pyhlist_end_py"] = None
        GlobalVar.name_to_postfunc["LinearRegressionModel#LinearR_load_pyhlist_end_py"] = LinearModel.load_pyhlist_end_postfunc

        GlobalVar.name_to_type["LinearRegressionModel#LinearR_load_pyhlist_py"] = GlobalVar.librarytype
        GlobalVar.name_to_type["LinearRegressionModel#LinearR_load_pyhlist_end_py"] = GlobalVar.librarytype

    # For those functions that need to load from PyHuskyList
    @staticmethod
    def load_pyhlist_prefunc(op):
        GlobalVar.xy_line_list = op.op_param[OperationParam.list_str]
        GlobalVar.is_sparse = op.op_param["is_sparse"]
        GlobalVar.xy_line_store = []
    @staticmethod
    def load_pyhlist_func(op, data):
        for I in data:
            assert type(I) is tuple and len(I) is 2
            X, y = I
            assert hasattr(X, '__iter__')
            GlobalVar.xy_line_store.append(I)
    @staticmethod
    def load_pyhlist_end_postfunc(op):
        GlobalSocket.pipe_to_cpp.send("LinearRegressionModel#LinearR_load_pyhlist_py")
        GlobalSocket.pipe_to_cpp.send(GlobalVar.xy_line_list)
        GlobalSocket.pipe_to_cpp.send(GlobalVar.is_sparse)
        # send out the len of datatset
        GlobalSocket.pipe_to_cpp.send(str(len(GlobalVar.xy_line_store)))
        for (X, y) in GlobalVar.xy_line_store:
            # send out the len of X
            GlobalSocket.pipe_to_cpp.send(str(len(X)))
            for i in X:
                GlobalSocket.pipe_to_cpp.send(str(i))
            GlobalSocket.pipe_to_cpp.send(str(y))
        GlobalSocket.xy_line_store = []
