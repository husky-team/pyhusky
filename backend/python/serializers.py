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

import cPickle
import marshal
import sys
import zlib
import msgpack

if sys.version < '3':
    import cPickle as pickle
    protocol = 1
else:
    import pickle
    protocol = 3

class MsgpackSerializer():

    def dumps(self, obj):
        return msgpack.packb(obj)

    def loads(self, obj):
        return msgpack.unpackb(obj)

class MarshalSerializer():

    def dumps(self, obj):
        return marshal.dumps(obj)

    def loads(self, obj):
        return marshal.loads(obj)

class PickleSerializer():

    def dumps(self, obj):
        return pickle.dumps(obj, protocol)

    if sys.version >= '3':
        def loads(self, obj, encoding="bytes"):
            return pickle.loads(obj, encoding=encoding)
    else:
        def loads(self, obj, encoding=None):
            return pickle.loads(obj)

class AutoSerializer():
    """
    Choose marshal or pickle as serialization protocol automatically
    """
    def __init__(self):
        self._type = None

    def dumps(self, obj):
        if self._type is not None:
            return b'P' + pickle.dumps(obj, -1)
        try:
            return b'M' + marshal.dumps(obj)
        except Exception:
            self._type = b'P'
            return b'P' + pickle.dumps(obj, -1)

    def loads(self, obj):
        _type = obj[0]
        if _type == b'M':
            return marshal.loads(obj[1:])
        elif _type == b'P':
            return pickle.loads(obj[1:])
        else:
            raise ValueError("invalid sevialization type: %s" % _type)

class CompressedSerializer():
    """
    Compress the serialized data
    """
    def __init__(self, serializer):
        self.serializer = serializer

    def dumps(self, obj):
        return zlib.compress(self.serializer.dumps(obj), 1)

    def loads(self, obj):
        return self.serializer.loads(zlib.decompress(obj))

class Serializer:
    serializer = PickleSerializer()
    # serializer = MarshalSerializer()
    # serializer = MsgpackSerializer()
    @staticmethod
    def dumps(obj):
        return Serializer.serializer.dumps(obj)

    @staticmethod
    def loads(s):
        return Serializer.serializer.loads(s)
