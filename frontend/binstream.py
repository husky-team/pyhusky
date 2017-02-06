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

import cloudpickle
import struct
import types

from operation import Operation

class BinStream(object):
    def __init__(self):
        self.data_buf = ""
        self.front = 0

    def __lshift__(self, obj):
        if isinstance(obj, types.IntType):
            self.data_buf += struct.pack('=i', obj)
        elif isinstance(obj, types.LongType):
            self.data_buf += struct.pack('=Q', obj)
        elif isinstance(obj, types.FloatType):
            self.data_buf += struct.pack('=f', obj)
        elif isinstance(obj, types.StringType):
            self << long(len(obj))
            self.data_buf += obj
        elif isinstance(obj, types.FunctionType):
            self << cloudpickle.dumps(obj)
        elif isinstance(obj, types.ListType):
            self << long(len(obj))
            for elem in obj:
                self << elem
        elif isinstance(obj, types.DictType):
            self << long(len(obj))
            for k,v in obj.iteritems():
                self << k
                self << v
        elif isinstance(obj, Operation):
            self << obj.op_name
            self << obj.op_param

    def load_int32(self):
        val = struct.unpack('=i', self.data_buf[self.front:self.front+4])[0]
        self.front += 4
        return val

    def load_int64(self):
        val = struct.unpack('=Q', self.data_buf[self.front:self.front+8])[0]
        self.front += 8
        return val
    
    def load_float(self):
        val = struct.unpack('=f', self.data_buf[self.front:self.front+4])[0]
        self.front += 4
        return val

    def load_double(self):
        val = struct.unpack('=d', self.data_buf[self.front:self.front+8])[0]
        self.front += 8
        return val

    def load_str(self):
        sz = self.load_int64()
        val = self.data_buf[self.front:self.front+sz]
        self.front += sz
        return val

    def load_func(self):
        func_str = self.load_str()
        return cloudpickle.loads(func_str)

    """
    Load an operation without its dependencies
    """
    def load_op(self):
        op_name = self.load_str()
        param_sz = self.load_int64()
        op_param = dict()
        for i in xrange(param_sz):
            # op_param.append(self.load_str())
            k, v = self.load_str(), self.load_str()
            op_param[k] = v
        op = Operation(op_name, op_param)
        return op

    def load_dag(self):
        op_dict = dict()
        deps = set()
        while self.size() != 0:
            id = self.load_int32()
            op_dict[id] = self.load_op()
            deps_sz = self.load_int64()
            for i in xrange(deps_sz):
                dep_id = self.load_int32()
                op_dict[id].op_deps.append(op_dict[dep_id])
                deps.add(dep_id)
        for id in op_dict:
            if id not in deps:
                return op_dict[id]

    def size(self):
        return len(self.data_buf) - self.front
