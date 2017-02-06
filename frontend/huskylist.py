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

import random
import string
import types

import config
import session
from operation import Operation, OperationParam 
from scheduler import compute, compute_collect

import copy

def gen_list_name():
    list_name_len = 8
    return ''.join(random.choice(string.ascii_lowercase+string.digits) for _ in range(list_name_len))

class HuskyList(object):
    def __init__(self):
        self.pending_op = None
        try: 
            self.list_name = config.conf.session_id + gen_list_name()
        except:
            print "Error: Cannot initialize HuskyList; maybe you forgot to use pyhusky_start()"
            exit()

class PyHuskyList(HuskyList):
    def __init__(self):
        super(PyHuskyList, self).__init__()

    def flat_map(self, func):
        if hasattr(func, '__call__'):
            phlist = PyHuskyList()
            param = {OperationParam.lambda_str : func,
                     OperationParam.list_str : phlist.list_name}
            phlist.pending_op = Operation("Functional#flat_map_py", param, [self.pending_op])
            return phlist
        else:
            return NotImplemented

    def map(self, func):
        if hasattr(func, '__call__'):
            phlist = PyHuskyList()
            param = {OperationParam.lambda_str : func,
                     OperationParam.list_str : phlist.list_name}
            phlist.pending_op = Operation("Functional#map_py", param, [self.pending_op])
            return phlist
        else:
            return NotImplemented

    def filter(self, func):
        if hasattr(func, '__call__'):
            phlist = PyHuskyList()
            param = {OperationParam.lambda_str : func,
                     OperationParam.list_str : phlist.list_name}
            phlist.pending_op = Operation("Functional#filter_py", param, [self.pending_op])
            return phlist
        else:
            return NotImplemented

    def reduce(self, func):
        if hasattr(func, '__call__'):
            param = {OperationParam.lambda_str : func,
                     OperationParam.list_str : self.list_name}
            op = Operation("Functional#reduce_py", param, [self.pending_op])
            return compute_collect(op)
        else:
            return NotImplemented

    def concat(self, other_list):
        if isinstance(other_list, PyHuskyList) or isinstance(other_list, HuskyListStr):
            phlist = PyHuskyList()
            param = {OperationParam.list_str : phlist.list_name}
            phlist.pending_op = Operation("Functional#concat_py", param, [self.pending_op, other_list.pending_op])
            return phlist
        else:
            return NotImplemented

    def cache(self):
        if self.pending_op.is_materialized is True:
            return self
        param = {OperationParam.list_str : self.list_name}
        op = Operation("Functional#cache_py", param, [self.pending_op])
        compute(op)
        self.pending_op.is_materialized = True
        return self

    def uncache(self):
        if self.pending_op.is_materialized is False:
            return None
        param = {OperationParam.list_str : self.list_name}
        op = Operation("Functional#uncache_py", param, [])
        compute(op)
        self.pending_op.is_materialized = False
        return None

    def reduce_by_key(self, func):
        if hasattr(func, '__call__'):
            phlist = PyHuskyList()
            param = {OperationParam.lambda_str : func,
                     OperationParam.list_str : phlist.list_name}
            phlist.pending_op = Operation("Functional#reduce_by_key_py", param, [self.pending_op])
            return phlist
        else:
            return NotImplemented

    def group_by_key(self):
        phlist = PyHuskyList()
        param = {OperationParam.list_str : phlist.list_name}
        phlist.pending_op = Operation("Functional#group_by_key_py", param, [self.pending_op])
        return phlist

    def write_to_hdfs(self, url): 
        param = {OperationParam.url_str : url, 
                 OperationParam.list_str : self.list_name }
        op = Operation("Functional#write_to_hdfs_py", param, [self.pending_op])
        compute(op)
        return None

    def count(self):
        param = {OperationParam.list_str : self.list_name}
        op = Operation("Functional#count_py", param, [self.pending_op])
        return compute_collect(op)

    def collect(self):
        param = {OperationParam.list_str : self.list_name}
        op = Operation("Functional#collect_py", param, [self.pending_op])
        return compute_collect(op)

    def distinct(self):
        phlist = PyHuskyList()
        param = {OperationParam.list_str : phlist.list_name}
        phlist.pending_op = Operation("Functional#distinct_py", param, [self.pending_op])
        return phlist

    def difference(self, other_list):
        if isinstance(other_list, PyHuskyList):
            phlist = PyHuskyList()
            param = {OperationParam.list_str : phlist.list_name}
            self.pending_op.op_param[phlist.list_name+"_diffl"] = "dummy"
            other_list.pending_op.op_param[phlist.list_name+"_diffr"] = "dummy"
            phlist.pending_op = Operation("Functional#difference_py", param, [self.pending_op, other_list.pending_op])
            return phlist
        else:
            return NotImplemented

    def map_partition(self, func):
        if hasattr(func, '__call__'):
            phlist = PyHuskyList()
            param = {OperationParam.lambda_str : func,
                     OperationParam.list_str : phlist.list_name}
            phlist.pending_op = Operation("Functional#map_partition_py", param, [self.pending_op])
            return phlist
        else:
            return NotImplemented

    def topk(self, k, key=lambda x:x, reverse=False):
        return self.map_partition(lambda a:sorted(a, None, key, reverse)[:k]).reduce(lambda a,b:sorted(a+b, None, key, reverse)[:k])
    
    def empty(self):
        return True if self.count() == 0 else False

    def output(self):
        print self.collect()

    def count_by_key(self):
        return self.map(lambda (k,v):(k,1)).reduce_by_key(lambda x,y:x+y)

    def foreach(self, func):
        if hasattr(func, '__call__'):
            param = {OperationParam.lambda_str : func,
                     OperationParam.list_str : self.list_name}
            op = Operation("Functional#foreach_py", param, [self.pending_op])
            compute(op)
            return
        else:
            return NotImplemented

class HDFS(PyHuskyList):
    def __init__(self, host=None, port=None):
        super(PyHuskyList, self).__init__()
        assert host is not None and port is not None
        self.host = host
        self.port = port

    def load(self, path=None):
        assert path is not None
        param = {
            "Protocol": "hdfs", 
            "Host": self.host, 
            "Port": self.port,
            "Path": path,
            OperationParam.list_str : self.list_name
        }
        self.pending_op = Operation("Functional#load_py", param, [])
        return self

class MongoDB(PyHuskyList):
    def __init__(self, host=None, port=None):
        super(PyHuskyList, self).__init__()
        assert host is not None and port is not None
        self.host = host
        self.port = port
        self.user = ''
        self.pwd = ''

    def auth(self, user=None, pwd=None):
        assert user is not None and pwd is not None
        self.user = user
        self.pwd = pwd
        return self

    def load(self, database=None, collection=None):
        assert database is not None and collection is not None
        param = {
            "Protocol": "mongodb", 
            "Server": '{}:{}'.format(self.host, self.port),
            "Database": database,
            "Collection": collection,
            "Username": self.user,
            "Password": self.pwd,
            OperationParam.list_str : self.list_name
        }
        self.pending_op = Operation("Functional#load_py", param, [])
        return self

