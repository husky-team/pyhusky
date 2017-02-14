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

from pyhusky.backend.globalvar import GlobalSocket, GlobalVar, GlobalN2NSocket
from pyhusky.backend.processor import handle_instr
from pyhusky.backend.register import register_func
from pyhusky.common.serializers import Serializer, PickleSerializer

def log_msg(msg):
    print msg
    sys.stdout.flush()

#  args: local_id, global_id, proc_id, num_worker, session_id
if __name__ == "__main__":
    GlobalVar.local_id = int(sys.argv[1])
    GlobalVar.global_id = int(sys.argv[2])
    GlobalVar.proc_id = int(sys.argv[3])
    GlobalVar.num_workers = int(sys.argv[4])
    GlobalVar.session_id = int(sys.argv[5])

    GlobalVar.disablePipeline = False

    Serializer.serializer = PickleSerializer()

    GlobalSocket.init_socket(str(GlobalVar.local_id), str(GlobalVar.proc_id), str(GlobalVar.session_id))
    register_func()

    # N2N
    GlobalN2NSocket.init_socket()
    while True:
        to_exit = handle_instr()
        if to_exit:
            break
