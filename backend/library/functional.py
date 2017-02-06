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
import cPickle
import cloudpickle
from backend.python.globalvar import GlobalVar, GlobalSocket, OperationParam, GlobalN2NSocket
from backend.python.serializers import Serializer
from frontend.binstream import BinStream

from itertools import groupby, islice
from operator import itemgetter

def register_all():
    # register actions
    Load.register()
    Reduce.register()
    Cache.register()
    UnCache.register()
    ReduceByKey.register()
    GroupByKey.register()
    WriteToHdfs.register()
    Count.register()
    Collect.register()
    Parallelize.register()
    Distinct.register()
    Difference.register()
    MapPartition.register()
    ForEach.register()

    # register transformations
    Map.register()
    Filter.register()
    FlatMap.register()

"""Actions:
    Load, Reduce, Cache, UnCache, ReduceByKey, GroupByKey, WriteToHdfs,
    Count, Collect, Parallelize, Distinct, Difference
"""
class Load:
    @staticmethod
    def register():
        if GlobalVar.disablePipeline == False:
            GlobalVar.name_to_func["Functional#load_py"] = Load.load
        else:
            GlobalVar.name_to_func["Functional#load_py"] = Load.load_disablepipeline
        GlobalVar.name_to_type["Functional#load_py"] = GlobalVar.loadtype

    @staticmethod
    def load(op):
        while True:
            chunk = GlobalSocket.pipe_from_cpp.recv()
            if not chunk:
                break
            else:
                GlobalSocket.pipe_to_cpp.send("")
                yield chunk.split("\n");

    @staticmethod
    def load_disablepipeline(op):
        # disable pipeline
        ret = []
        while True:
            chunk = GlobalSocket.pipe_from_cpp.recv()
            if not chunk:
                break
            else:
                ret.append(chunk)
        yield ret

class Reduce:
    @staticmethod
    def register():
        GlobalVar.name_to_func["Functional#reduce_py"] = Reduce.func
        GlobalVar.name_to_prefunc["Functional#reduce_py"] = Reduce.prefunc
        GlobalVar.name_to_postfunc["Functional#reduce_py"] = Reduce.postfunc
        GlobalVar.name_to_type["Functional#reduce_py"] = GlobalVar.actiontype
        # end
        GlobalVar.name_to_postfunc["Functional#reduce_end_py"] = Reduce.end_postfunc
        GlobalVar.name_to_type["Functional#reduce_end_py"] = GlobalVar.actiontype
    @staticmethod
    def func(op, data):
        if not data:
            return
        res = reduce(op.func, data)
        op.res = res if op.res is None else op.func(op.res, res)
    @staticmethod
    def prefunc(op):
        op.func = cloudpickle.loads(op.op_param[OperationParam.lambda_str]) # store lambda
        if not hasattr(GlobalVar, 'reduce_res'):
            GlobalVar.reduce_res = None
        GlobalVar.reduce_func = staticmethod(op.func)
        op.res = None
    @staticmethod
    def postfunc(op):
        # store reduce_res
        if GlobalVar.reduce_res is None:
            GlobalVar.reduce_res = op.res
        elif op.res is None:
            pass
        else:
            GlobalVar.reduce_res = op.func(GlobalVar.reduce_res, op.res)

    @staticmethod
    def end_postfunc(op):
        GlobalSocket.pipe_to_cpp.send("Functional#reduce_end")
        GlobalSocket.pipe_to_cpp.send(Serializer.dumps(GlobalVar.reduce_res))
        res = None
        while True:
            recv_str = GlobalSocket.pipe_from_cpp.recv()
            # fdebug.write("recv: "+recv_str+"\n")
            if not recv_str:
                break
            recv = Serializer.loads(recv_str)
            if recv is None:
                continue
            if res is None:
                res = recv
            else:
                res = GlobalVar.reduce_func(res, recv)
        # fdebug.write("result: "+str(res)+"\n");
        GlobalSocket.pipe_to_cpp.send(Serializer.dumps(res))
        GlobalVar.reduce_res = None
        GlobalVar.reduce_func = None

class Count:
    @staticmethod
    def register():
        GlobalVar.name_to_func["Functional#count_py"] = Count.func
        GlobalVar.name_to_prefunc["Functional#count_py"] = Count.prefunc
        GlobalVar.name_to_postfunc["Functional#count_py"] = Count.postfunc
        GlobalVar.name_to_type["Functional#count_py"] = GlobalVar.actiontype
        # end
        GlobalVar.name_to_postfunc["Functional#count_end_py"] = Count.end_postfunc
        GlobalVar.name_to_type["Functional#count_end_py"] = GlobalVar.actiontype
    @staticmethod
    def prefunc(op):
        op.res = 0
        if not hasattr(GlobalVar, 'count_res'):
            GlobalVar.count_res = 0
    @staticmethod
    def func(op, data):
        op.res += len(data)
    @staticmethod
    def postfunc(op):
        GlobalVar.count_res += op.res
    @staticmethod
    def end_postfunc(op):
        GlobalSocket.pipe_to_cpp.send("Functional#count_end")
        GlobalSocket.pipe_to_cpp.send(str(GlobalVar.count_res))
        GlobalVar.count_res = 0

class Collect:
    @staticmethod
    def register():
        GlobalVar.name_to_func["Functional#collect_py"] = Collect.func
        GlobalVar.name_to_prefunc["Functional#collect_py"] = Collect.prefunc
        GlobalVar.name_to_type["Functional#collect_py"] = GlobalVar.actiontype
        # end
        GlobalVar.name_to_postfunc["Functional#collect_end_py"] = Collect.end_postfunc
        GlobalVar.name_to_type["Functional#collect_end_py"] = GlobalVar.actiontype
    @staticmethod
    def prefunc(op):
        if "collect_list" not in GlobalVar.data_chunk:
            GlobalVar.data_chunk["collect_list"] = []
    @staticmethod
    def func(op, data):
        GlobalVar.data_chunk["collect_list"].extend(data)
    @staticmethod
    def end_postfunc(op):
        GlobalSocket.pipe_to_cpp.send("Functional#collect_end")
        GlobalSocket.pipe_to_cpp.send("collect_list")
        GlobalSocket.pipe_to_cpp.send(Serializer.dumps(GlobalVar.data_chunk["collect_list"]))
        del GlobalVar.data_chunk["collect_list"]

class MapPartition:
    @staticmethod
    def register():
        GlobalVar.name_to_prefunc["Functional#map_partition_py"] = MapPartition.prefunc
        GlobalVar.name_to_func["Functional#map_partition_py"] = MapPartition.func
        GlobalVar.name_to_func["Functional#load_map_partition_py"] = MapPartition.load
        GlobalVar.name_to_type["Functional#map_partition_py"] = GlobalVar.actiontype

    @staticmethod
    def prefunc(op):
        op.func = cloudpickle.loads(op.op_param[OperationParam.lambda_str]) # store lambda
        if op.op_param[OperationParam.list_str] not in GlobalVar.data_chunk:
            GlobalVar.data_chunk[op.op_param[OperationParam.list_str]] = []
    @staticmethod
    def func(op, data):
        GlobalVar.data_chunk[op.op_param[OperationParam.list_str]].extend(data)
    @staticmethod
    def load(op):
        partition = GlobalVar.data_chunk[op.op_param[OperationParam.list_str]]
        print "[Debug] parition "+str(partition)
        ret = op.func(partition)
        GlobalVar.data_chunk[op.op_param[OperationParam.list_str]] = []
        print "[Debug] ret "+str(ret)
        assert type(ret) is list
        return [ret]

class Cache:
    @staticmethod
    def register():
        GlobalVar.name_to_func["Functional#cache_py"] = Cache.func
        GlobalVar.name_to_prefunc["Functional#cache_py"] = Cache.prefunc
        GlobalVar.name_to_type["Functional#cache_py"] = GlobalVar.actiontype

        if GlobalVar.disablePipeline == False:
            GlobalVar.name_to_func["Functional#load_cache_py"] = Cache.load
        else:
            GlobalVar.name_to_func["Functional#load_cache_py"] = Cache.load_disablepipeline
        GlobalVar.name_to_type["Functional#load_cache_py"] = GlobalVar.loadtype

    @staticmethod
    def func(op, data):
        GlobalVar.data_chunk[op.op_param[OperationParam.list_str]].extend(data)

    @staticmethod
    def prefunc(op):
        if op.op_param[OperationParam.list_str] not in GlobalVar.data_chunk:
            GlobalVar.data_chunk[op.op_param[OperationParam.list_str]] = []

    @staticmethod
    def load(op):
        for chunk in GlobalVar.data_chunk[op.op_param[OperationParam.list_str]]:
            yield [chunk]

    @staticmethod
    def load_disablepipeline(op):
        # disable pipeline
        yield GlobalVar.data_chunk[op.op_param[OperationParam.list_str]]


class UnCache:
    @staticmethod
    def register():
        GlobalVar.name_to_postfunc["Functional#uncache_py"] = UnCache.postfunc
        GlobalVar.name_to_type["Functional#uncache_py"] = GlobalVar.actiontype
    @staticmethod
    def postfunc(op):
        GlobalVar.data_chunk[op.op_param[OperationParam.list_str]] = None

class ReduceByKey:
    @staticmethod
    def register():
        GlobalVar.name_to_func["Functional#reduce_by_key_py"] = ReduceByKey.func_hashmap
        GlobalVar.name_to_prefunc["Functional#reduce_by_key_py"] = ReduceByKey.prefunc_hashmap
        GlobalVar.name_to_postfunc["Functional#reduce_by_key_py"] = ReduceByKey.postfunc_combine_n2n
        GlobalVar.name_to_type["Functional#reduce_by_key_py"] = GlobalVar.actiontype

        if GlobalVar.disablePipeline == False:
            GlobalVar.name_to_func["Functional#reduce_by_key_end_py"] = ReduceByKey.load_n2n
        else:
            GlobalVar.name_to_func["Functional#reduce_by_key_end_py"] = ReduceByKey.load_disablepipeline
        GlobalVar.name_to_prefunc["Functional#reduce_by_key_end_py"] = ReduceByKey.end_prefunc
        GlobalVar.name_to_type["Functional#reduce_by_key_end_py"] = GlobalVar.loadtype
    @staticmethod
    def func(op, data):
        for x in data:
            assert (type(x) is tuple or type(x) is list) and len(x) is 2
            # GlobalVar.reduce_by_key_store.append((Serializer.return dumps(x[0]), Serializer.dumps(x[1])))
            GlobalVar.reduce_by_key_store.append((x[0], x[1]))
        # import sys
        # print "data size: ", len(data)
        # sys.stdout.flush()
        # i = 0
        # for x in data:
        #     print i
        #     i+=1
        #     sys.stdout.flush()
        #     GlobalVar.reduce_by_key_store << Serializer.dumps(x[0])
        #     GlobalVar.reduce_by_key_store << Serializer.dumps(x[1])
        # print "data done"
        # sys.stdout.flush()
    @staticmethod
    def prefunc(op):
        GlobalVar.reduce_by_key_store = []
        # disable pipeline
        # GlobalVar.reduce_by_key_store = BinStream()
        GlobalVar.reduce_by_key_list = op.op_param[OperationParam.list_str]
        op.func = cloudpickle.loads(op.op_param[OperationParam.lambda_str]) # store lambda
    @staticmethod
    def postfunc_combine_1(op):
        # send out reduce_by_key_store
        GlobalSocket.pipe_to_cpp.send("Functional#reduce_by_key")
        GlobalSocket.pipe_to_cpp.send(GlobalVar.reduce_by_key_list)

        # combine
        GlobalVar.reduce_by_key_store.sort(key=lambda x:x[0])
        send_buffer = []
        if GlobalVar.reduce_by_key_store:
            prev_x, prev_y = GlobalVar.reduce_by_key_store[0]
            for x,y in islice(GlobalVar.reduce_by_key_store, 1, None):
                if x != prev_x:
                    send_buffer.append(Serializer.dumps(prev_x))
                    send_buffer.append(Serializer.dumps(prev_y))
                    prev_x, prev_y = x,y
                else:
                    prev_y = op.func(prev_y, y)
            send_buffer.append(Serializer.dumps(prev_x))
            send_buffer.append(Serializer.dumps(prev_y))
        GlobalSocket.pipe_to_cpp.send(str(len(send_buffer)/2))
        for x in send_buffer:
            GlobalSocket.pipe_to_cpp.send(x)

# N2N
    @staticmethod
    def func_hashmap(op, data):
        store = GlobalVar.reduce_by_key_store
        for x in data:
            assert (type(x) is tuple or type(x) is list) and len(x) is 2
            if store.has_key(x[0]):
                store[x[0]] = op.func(x[1], store[x[0]])
            else:
                store[x[0]] = x[1]

    @staticmethod
    def prefunc_hashmap(op):
        GlobalVar.reduce_by_key_store = {}
        GlobalVar.reduce_by_key_list = op.op_param[OperationParam.list_str]
        op.func = cloudpickle.loads(op.op_param[OperationParam.lambda_str]) # store lambda


    @staticmethod
    def postfunc_combine_n2n(op):
        # send out reduce_by_key_store
        GlobalSocket.pipe_to_cpp.send("Functional#reduce_by_key")
        GlobalSocket.pipe_to_cpp.send(GlobalVar.reduce_by_key_list)

        """ Attempt4: Hash Map """
        GlobalSocket.pipe_to_cpp.send("0")
        send_buffer = [[] for i in xrange(GlobalVar.num_workers)]
        if GlobalVar.reduce_by_key_store:
            for x,y in GlobalVar.reduce_by_key_store.items():
                dst = hash(x) % GlobalVar.num_workers
                send_buffer[dst].append((x, y))
        for i in xrange(GlobalVar.num_workers):
            GlobalN2NSocket.send(i, Serializer.dumps(send_buffer[i]))

    @staticmethod
    def load_n2n(op):
        GlobalSocket.pipe_to_cpp.send("Functional#reduce_by_key_end")
        GlobalSocket.pipe_to_cpp.send(op.op_param[OperationParam.list_str])

        store = Serializer.loads(GlobalN2NSocket.recv())
        for i in xrange(1, GlobalVar.num_workers):
            store.extend(Serializer.loads(GlobalN2NSocket.recv()))

        """ Attempt 1: init """
        func = op.func
        store.sort(key=lambda x:x[0])
        if store:
            prev_x, prev_y = store[0]
            for x,y in islice(store, 1, None):
                if x != prev_x:
                    # buff.append((prev_x, prev_y))
                    yield [(prev_x, prev_y)]
                    prev_x, prev_y = x, y
                else:
                    prev_y = func(prev_y, y)
            # buff.append((prev_x, prev_y))
            yield [(prev_x, prev_y)]

# N2N

    @staticmethod
    def postfunc_combine_2(op):
        # send out reduce_by_key_store
        GlobalSocket.pipe_to_cpp.send("Functional#reduce_by_key")
        GlobalSocket.pipe_to_cpp.send(GlobalVar.reduce_by_key_list)

        # combine
        send_buffer = []
        # reduce_func = lambda x,y: x[0], op.func(x[1],y[1])
        def reduce_func(x,y):
            return x[0], op.func(x[1], y[1])
        for _,y in groupby(sorted(GlobalVar.reduce_by_key_store), key=lambda x:x[0]):
            k,v = reduce(reduce_func, y)
            send_buffer.append(Serializer.dumps(k))
            send_buffer.append(Serializer.dumps(v))
        GlobalSocket.pipe_to_cpp.send(str(len(send_buffer)/2))
        for x in send_buffer:
            GlobalSocket.pipe_to_cpp.send(x)

    @staticmethod
    def postfunc_combine_hash(op):
        # send out reduce_by_key_store
        GlobalSocket.pipe_to_cpp.send("Functional#reduce_by_key")
        GlobalSocket.pipe_to_cpp.send(GlobalVar.reduce_by_key_list)

        send_buffer = dict()
        for x,y in GlobalVar.reduce_by_key_store:
            if x in send_buffer:
                send_buffer[x] = op.func(send_buffer[x], y)
            else:
                send_buffer[x] = y
        GlobalSocket.pipe_to_cpp.send(str(len(send_buffer)))
        for x,y in send_buffer.iteritems():
            GlobalSocket.pipe_to_cpp.send(Serializer.dumps(x))
            GlobalSocket.pipe_to_cpp.send(Serializer.dumps(y))

    @staticmethod
    def postfunc_no_combine(op):
        GlobalSocket.pipe_to_cpp.send("Functional#reduce_by_key")
        GlobalSocket.pipe_to_cpp.send(GlobalVar.reduce_by_key_list)
        GlobalSocket.pipe_to_cpp.send(str(len(GlobalVar.reduce_by_key_store)))
        for (x,y) in GlobalVar.reduce_by_key_store:
            GlobalSocket.pipe_to_cpp.send(Serializer.dumps(x))
            GlobalSocket.pipe_to_cpp.send(Serializer.dumps(y))
        GlobalVar.reduce_by_key_store = []

        # GlobalSocket.pipe_to_cpp.send("reduce_by_key")
        # GlobalSocket.pipe_to_cpp.send(GlobalVar.reduce_by_key_list)
        # GlobalSocket.pipe_to_cpp.send(GlobalVar.reduce_by_key_store.data_buf)
        # GlobalVar.reduce_by_key_store = None

    @staticmethod
    def end_prefunc(op):
        op.func = cloudpickle.loads(op.op_param[OperationParam.lambda_str]) # store lambda
    @staticmethod
    def load(op):
        GlobalSocket.pipe_to_cpp.send("Functional#reduce_by_key_end")
        GlobalSocket.pipe_to_cpp.send(op.op_param[OperationParam.list_str])
        func = op.func
        while(True):
            key = GlobalSocket.pipe_from_cpp.recv()
            if not key:
                break
            key = Serializer.loads(key)
            num = int(GlobalSocket.pipe_from_cpp.recv())
            res = None
            for i in xrange(num):
                recv = Serializer.loads(GlobalSocket.pipe_from_cpp.recv())
                res = recv if res is None else func(res, recv)
            # print "reduce_by_key res: ", key, res

            yield [(key, res)]

    @staticmethod
    def load_disablepipeline(op):
        GlobalSocket.pipe_to_cpp.send("Functional#reduce_by_key_end")
        GlobalSocket.pipe_to_cpp.send(op.op_param[OperationParam.list_str])
        func = op.func
        ret = []
        while(True):
            key = GlobalSocket.pipe_from_cpp.recv()
            if not key:
                break
            key = Serializer.loads(key)
            num = int(GlobalSocket.pipe_from_cpp.recv())
            res = None
            for i in xrange(num):
                recv = Serializer.loads(GlobalSocket.pipe_from_cpp.recv())
                res = recv if res is None else func(res, recv)
            # print "reduce_by_key res: ", key, res
            ret.append((key, res))
        yield ret

class GroupByKey:
    @staticmethod
    def register():
        GlobalVar.name_to_func["Functional#group_by_key_py"] = GroupByKey.func
        GlobalVar.name_to_prefunc["Functional#group_by_key_py"] = GroupByKey.prefunc
        GlobalVar.name_to_postfunc["Functional#group_by_key_py"] = GroupByKey.postfunc
        GlobalVar.name_to_type["Functional#group_by_key_py"] = GlobalVar.actiontype

        GlobalVar.name_to_func["Functional#group_by_key_end_py"] = GroupByKey.load
        GlobalVar.name_to_type["Functional#group_by_key_end_py"] = GlobalVar.loadtype
    @staticmethod
    def func(op, data):
        for x in data:
            assert (type(x) is tuple or type(x) is list) and len(x) is 2
            GlobalVar.group_by_key_store.append((Serializer.dumps(x[0]), Serializer.dumps(x[1])))
    @staticmethod
    def prefunc(op):
        GlobalVar.group_by_key_store = []
        GlobalVar.group_by_key_list = op.op_param[OperationParam.list_str]
    @staticmethod
    def postfunc(op):
        # send out group_by_key_store
        GlobalSocket.pipe_to_cpp.send("Functional#group_by_key")
        GlobalSocket.pipe_to_cpp.send(GlobalVar.group_by_key_list)
        GlobalSocket.pipe_to_cpp.send(str(len(GlobalVar.group_by_key_store)))
        for (x,y) in GlobalVar.group_by_key_store:
            GlobalSocket.pipe_to_cpp.send(x)
            GlobalSocket.pipe_to_cpp.send(y)
        GlobalVar.group_by_key_store = []

    @staticmethod
    def load(op):
        GlobalSocket.pipe_to_cpp.send("Functional#group_by_key_end")
        GlobalSocket.pipe_to_cpp.send(op.op_param[OperationParam.list_str])
        while(True):
            key = GlobalSocket.pipe_from_cpp.recv()
            if not key:
                break
            key = Serializer.loads(key)
            num = int(GlobalSocket.pipe_from_cpp.recv())
            res = []
            for i in xrange(num):
                recv = Serializer.loads(GlobalSocket.pipe_from_cpp.recv())
                res.append(recv)
            # print "group_by_key res: ", key, res
            yield [(key, res)]

class Distinct:
    @staticmethod
    def register():
        GlobalVar.name_to_func["Functional#distinct_py"] = Distinct.func
        GlobalVar.name_to_prefunc["Functional#distinct_py"] = Distinct.prefunc
        GlobalVar.name_to_postfunc["Functional#distinct_py"] = Distinct.postfunc
        GlobalVar.name_to_type["Functional#distinct_py"] = GlobalVar.actiontype

        GlobalVar.name_to_func["Functional#distinct_end_py"] = Distinct.load
        GlobalVar.name_to_type["Functional#distinct_end_py"] = GlobalVar.loadtype
    @staticmethod
    def func(op, data):
        for x in data:
            GlobalVar.distinct_store.append(Serializer.dumps(x))
    @staticmethod
    def prefunc(op):
        GlobalVar.distinct_store = []
    @staticmethod
    def postfunc(op):
        # send out distinct_store
        GlobalSocket.pipe_to_cpp.send("Functional#distinct")
        GlobalSocket.pipe_to_cpp.send(op.op_param[OperationParam.list_str])
        GlobalSocket.pipe_to_cpp.send(str(len(GlobalVar.distinct_store)))
        for x in GlobalVar.distinct_store:
            GlobalSocket.pipe_to_cpp.send(x)
        GlobalVar.distinct_store = []

    @staticmethod
    def load(op):
        GlobalSocket.pipe_to_cpp.send("Functional#distinct_end")
        GlobalSocket.pipe_to_cpp.send(op.op_param[OperationParam.list_str])
        while(True):
            value = GlobalSocket.pipe_from_cpp.recv()
            if not value:
                break
            value = Serializer.loads(value)
            yield [value]

class Difference:
    @staticmethod
    def register():
        GlobalVar.name_to_func["Functional#difference_py"] = Difference.func
        GlobalVar.name_to_prefunc["Functional#difference_py"] = Difference.prefunc
        GlobalVar.name_to_postfunc["Functional#difference_py"] = Difference.postfunc
        GlobalVar.name_to_type["Functional#difference_py"] = GlobalVar.actiontype

        GlobalVar.name_to_func["Functional#difference_end_py"] = Difference.load
        GlobalVar.name_to_type["Functional#difference_end_py"] = GlobalVar.loadtype

    @staticmethod
    def prefunc(op):
        GlobalVar.difference_store = []
    @staticmethod
    def func(op, data):
        for x in data:
            GlobalVar.difference_store.append(Serializer.dumps(x))
    @staticmethod
    def postfunc(op):
        # send out difference_store
        GlobalSocket.pipe_to_cpp.send("Functional#difference")
        GlobalSocket.pipe_to_cpp.send(op.op_param[OperationParam.list_str])
        GlobalSocket.pipe_to_cpp.send(op.op_param[op.op_param[OperationParam.list_str]+"_diff_type"]) # left/right
        GlobalSocket.pipe_to_cpp.send(str(len(GlobalVar.difference_store)))
        for x in GlobalVar.difference_store:
            GlobalSocket.pipe_to_cpp.send(x)
        GlobalVar.difference_store = []

    @staticmethod
    def load(op):
        GlobalSocket.pipe_to_cpp.send("Functional#difference_end")
        GlobalSocket.pipe_to_cpp.send(op.op_param[OperationParam.list_str])
        while(True):
            value = GlobalSocket.pipe_from_cpp.recv()
            if not value:
                break
            value = Serializer.loads(value)
            cnt = int(GlobalSocket.pipe_from_cpp.recv())
            yield [value]

class Parallelize:
    @staticmethod
    def register():
        GlobalVar.name_to_func["Functional#parallelize_py"] = Parallelize.load
        GlobalVar.name_to_type["Functional#parallelize_py"] = GlobalVar.loadtype
    @staticmethod
    def load(op):
        data = Serializer.loads(op.op_param[OperationParam.data_str])
        i = GlobalVar.global_id
        while i < len(data):
            # print data[i]
            yield [data[i]]
            i += GlobalVar.num_workers

class WriteToHdfs:
    @staticmethod
    def register():
        GlobalVar.name_to_func["Functional#write_to_hdfs_py"] = WriteToHdfs.func
        GlobalVar.name_to_prefunc["Functional#write_to_hdfs_py"] = WriteToHdfs.prefunc
        GlobalVar.name_to_postfunc["Functional#write_to_hdfs_py"] = WriteToHdfs.postfunc
        GlobalVar.name_to_type["Functional#write_to_hdfs_py"] = GlobalVar.actiontype
    @staticmethod
    def func(op, data):
        for x in data:
            # GlobalVar.write_to_hdfs_store.append(repr(x)+"\n")  # May use this when writing repr data
            GlobalVar.write_to_hdfs_store.append(str(x)+"\n")
    @staticmethod
    def prefunc(op):
        GlobalVar.write_to_hdfs_store = []
        GlobalVar.write_to_hdfs_list = op.op_param[OperationParam.list_str]
        GlobalVar.write_to_hdfs_url = op.op_param[OperationParam.url_str]
    @staticmethod
    def postfunc(op):
        GlobalSocket.pipe_to_cpp.send("Functional#write_to_hdfs")
        GlobalSocket.pipe_to_cpp.send(GlobalVar.write_to_hdfs_list)
        GlobalSocket.pipe_to_cpp.send(GlobalVar.write_to_hdfs_url)
        GlobalSocket.pipe_to_cpp.send(str(len(GlobalVar.write_to_hdfs_store)))
        for x in GlobalVar.write_to_hdfs_store:
            GlobalSocket.pipe_to_cpp.send(x)
        GlobalVar.write_to_hdfs_store = []

class ForEach():
    @staticmethod
    def register():
        GlobalVar.name_to_func["Functional#foreach_py"] = ForEach.func
        GlobalVar.name_to_prefunc["Functional#foreach_py"] = ForEach.prefunc
        GlobalVar.name_to_type["Functional#foreach_py"] = GlobalVar.actiontype
    @staticmethod
    def func(op, data):
        for x in data:
            op.func(x)
    @staticmethod
    def prefunc(op):
        op.func = cloudpickle.loads(op.op_param[OperationParam.lambda_str]) # store lambda

"""Transformations:
    Map, Filter, FlatMap
"""
class Map:
    @staticmethod
    def register():
        GlobalVar.name_to_func["Functional#map_py"] = Map.func
        GlobalVar.name_to_prefunc["Functional#map_py"] = Map.prefunc
        GlobalVar.name_to_type["Functional#map_py"] = GlobalVar.transformationtype
    @staticmethod
    def func(op, data):
        ret = [op.func(x) for x in data]
        return ret
    @staticmethod
    def prefunc(op):
        op.func = cloudpickle.loads(op.op_param[OperationParam.lambda_str]) # store lambda

class Filter:
    @staticmethod
    def register():
        GlobalVar.name_to_func["Functional#filter_py"] = Filter.func
        GlobalVar.name_to_prefunc["Functional#filter_py"] = Filter.prefunc
        GlobalVar.name_to_type["Functional#filter_py"] = GlobalVar.transformationtype
    @staticmethod
    def func(op, data):
        ret = [x for x in data if op.func(x)]
        return ret
    @staticmethod
    def prefunc(op):
        op.func = cloudpickle.loads(op.op_param[OperationParam.lambda_str]) # store lambda

class FlatMap:
    @staticmethod
    def register():
        GlobalVar.name_to_func["Functional#flat_map_py"] = FlatMap.func
        GlobalVar.name_to_prefunc["Functional#flat_map_py"] = FlatMap.prefunc
        GlobalVar.name_to_type["Functional#flat_map_py"] = GlobalVar.transformationtype
    @staticmethod
    def func(op, data):
        ret = []
        for x in data:
            ret += op.func(x)
            # ret.extend(op.func(x))
        return ret
    @staticmethod
    def prefunc(op):
        op.func = cloudpickle.loads(op.op_param[OperationParam.lambda_str]) # store lambda
