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

import atexit
import argparse
import random
import string

import session

disable_progress = False

def log_msg(msgs):
    if disable_progress:
        return
    print msgs


class Config:
    """self.params is used for pass some parameters to frontend.
    params:
        disable_progress:True/False (False by default)
        ...
    ``examples``
    pyhusky_start(params={disable_progress:True})

    """
    def __init__(self):
        self.master_host = None
        self.master_port = None
        self.session_id = None
        self._params = None

    def __init__(self, _master_host, _master_port, _params={}):
        """master_host and master_port can be provided in two ways:
        1, use command line arguments, and then use pyhusky_start()
        2, use pyhusky_start(master_host, master_port)
        """
        if _master_host is None and _master_port is None:
            parser = argparse.ArgumentParser()
            parser.add_argument('--host', type=str, required=True)
            parser.add_argument('--port', type=str, required=True)
            self.master_host = parser.parse_args().host
            self.master_port = parser.parse_args().port
        else:
            self.master_host = _master_host
            self.master_port = str(_master_port)
        assert self.master_host is not None and self.master_port is not None

        session_id_len = 8
        self.session_id = ''.join(random.choice(string.ascii_lowercase+string.digits) for _ in range(session_id_len))

        assert type(_params) is dict
        self.params = _params
        global disable_progress
        disable_progress = self.params.get("disable_progress", False)

        session.new_session(self.master_host, self.master_port, self.session_id)
        log_msg("Connected to Master")
        atexit.register(session.end_session)

    def __repr__(self):
        return ("master_host: " + self.master_host
              + "\nmaster_port: " + self.master_port
              + "\nsession_id: " + self.session_id)

conf = None
