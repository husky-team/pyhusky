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

import communication
import time

def new_session(master_host, master_port, session_id):
    communication.init(b'fe'+session_id, "tcp://"+master_host+":"+master_port)
    communication.send(communication.TYPE_SESSION_BEGIN_PY)

def end_session():
    time.sleep(1)
    communication.send(communication.TYPE_SESSION_END_PY)
