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
