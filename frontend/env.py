import cPickle

import config
import session
from huskylist import PyHuskyList, HDFS, MongoDB
from operation import Operation, OperationParam
from datareceiver import Receiver
from backend.python.serializers import Serializer, MarshalSerializer, PickleSerializer, AutoSerializer, CompressedSerializer

# three types of lists:
# HuskyList, PyHuskyList, HuskyObjList

def pyhusky_start(master_host=None, master_port=None, params={}):
    # create a config file
    config.conf = config.Config(master_host, master_port, params)

    # Reigster receiver
    Receiver.register()

    # Set serializer {MarshalSerializer(), PickleSerializer(), AutoSerializer(), CompressedSerializer(PickleSerializer())}
    Serializer.serializer = PickleSerializer()

def pyhusky_stop():
    config.conf = None
    session.end_session()


def load(path):
    # hlist = HuskyListStr()
    # In this case the list represents a list of std::string
    hlist = PyHuskyList()
    param = {
        "Type": "cpp",
        "Path": path,
        OperationParam.list_str: hlist.list_name
    }
    if path.startswith("nfs:"):
        param["Protocol"] = "nfs"
    elif path.startswith("hdfs"):
        param["Protocol"] = "hdfs"
    else:
        raise Exception("ERROR: Cannot resolve the protocol of the load path")
    hlist.pending_op = Operation("Functional#load_py", param, [])
    return hlist


def hdfs(host=None, port=None):
    assert host is not None and port is not None
    return HDFS(host, port)


def mongodb(host=None, port=None):
    assert host is not None and port is not None
    return MongoDB(host, port)


def parallelize(data):
    if type(data) is list:
        hlist = PyHuskyList()
        pdata = cPickle.dumps(data)
        param = {OperationParam.data_str: pdata,
                 OperationParam.list_str: hlist.list_name}
        hlist.pending_op = Operation("Functional#parallelize_py", param, [])
        return hlist
    else:
        return NotImplemented
