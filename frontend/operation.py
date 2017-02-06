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

import types

class OperationParam:
    data_str = "data"
    url_str = "url"
    lambda_str = "lambda"
    list_str = "list_name"

class Operation:
    def __init__(self, op_name, op_param, op_deps=[]):
        self.op_name = op_name
        # assert isinstance(op_param, types.ListType)
        # self.op_param = op_param
        assert isinstance(op_param, types.DictType)
        self.op_param = op_param
        self.op_deps = [dep for dep in op_deps if dep is not None]
        self.is_materialized = False
    def __repr__(self):
        return "<"+self.op_name+">"
        # return "<"+self.op_name+">"+" ".join(str(x) for x in self.op_param)
