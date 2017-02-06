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

import zmq
import sys

class OperationParam:
    data_str = "data"
    url_str = "url"
    lambda_str = "lambda"
    list_str = "list_name"

class GlobalVar:
    actiontype = "action"
    transformationtype = "transformation"
    loadtype = "load"
    librarytype = "library"
    data_chunk = dict()
    name_to_func = dict()
    name_to_type = dict()
    name_to_prefunc = dict()
    name_to_postfunc = dict()

class GlobalSocket:
    # pipe_from_cpp
    # pipe_to_cpp
    @staticmethod
    def init_socket(wid, pid, session_id):
        ctx = zmq.Context()
        GlobalSocket.pipe_from_cpp = zmq.Socket(ctx, zmq.PULL)
        GlobalSocket.pipe_from_cpp.bind("ipc://pyhusky-session-"+session_id+"-proc-"+pid+"-"+wid)
        GlobalSocket.pipe_to_cpp = zmq.Socket(ctx, zmq.PUSH)
        GlobalSocket.pipe_to_cpp.connect("ipc://cpphusky-session-"+session_id+"-proc-"+pid+"-"+wid)

    @staticmethod
    def send(content):
        GlobalSocket.pipe_to_cpp.send(content)

    @staticmethod
    def recv():
        return GlobalSocket.pipe_from_cpp.recv()

# N2N
import threading

class GlobalN2NSocket:
    @staticmethod
    def init_socket():
        ctx = zmq.Context()
        comm_port = int(GlobalSocket.recv()) + 1
        GlobalN2NSocket.puller = ctx.socket(zmq.PULL)
        GlobalN2NSocket.puller.bind("tcp://0.0.0.0:" + str(comm_port + GlobalVar.local_id))
        GlobalN2NSocket.pushers = []
        for i in xrange(int(GlobalSocket.recv())):
            host = "tcp://" + GlobalSocket.recv() + ":"
            for j in xrange(int(GlobalSocket.recv())):
                sock = ctx.socket(zmq.PUSH)
                sock.connect(host + str(comm_port + j))
                GlobalN2NSocket.pushers.append(sock)

    @staticmethod
    def send(dst, msg):
        GlobalN2NSocket.pushers[dst].send(msg)

    @staticmethod
    def recv():
        return GlobalN2NSocket.puller.recv()

# N2N
