from binstream import BinStream
from frontend.library.register import register
from backend.python.serializers import Serializer

class Receiver:
    receiver_map = dict()
    @staticmethod
    def register():
        register(Receiver.receiver_map)

    @staticmethod
    def pythonbackend_receiver(reply):
        data = reply.load_str()
        return Serializer.loads(data)

def data_receiver(reply, op):
    flag = reply.load_int32()
    if flag == 0: # from python
        return Receiver.pythonbackend_receiver(reply)
    else: # from c++
        return Receiver.receiver_map[op.op_name](reply)
