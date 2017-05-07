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

from pyhusky.common.operation import Operation, OperationParam
from pyhusky.frontend import scheduler
from pyhusky.frontend.huskylist import HuskyList
from pyhusky.frontend.huskylist import PyHuskyList

class Word(HuskyList):
    """A Word class to represent words in C++
    """

    def __init__(self):
        super(Word, self).__init__()
        self.list_name += "Word"
        self._loaded = False
        self._computed = False
    
    def load_phlist(self, wordlist):
        assert self._loaded == False, "The words were already loaded"
        assert isinstance(wordlist, PyHuskyList)
        param = {OperationParam.list_str : self.list_name}
        op = Operation("Word#load_phlist_py", param, [wordlist.pending_op])
        scheduler.compute(op)
        self._loaded = True

    def load_hdfs(self, url):
        assert self._loaded == False, "The words were already loaded"
        assert type(url) is str
        param = {OperationParam.list_str : self.list_name,
                "url" : url,
                "Type" : "cpp"}
        op = Operation("Word#load_hdfs_py", param, [])
        scheduler.compute(op)
        self._loaded = True

    def wordcount(self):
        """wordcount function is to compute wordcount using the C++ library
        """
        assert self._loaded == True, "Words are not loaded"
        param = {OperationParam.list_str : self.list_name,
                "Type" : "cpp"}
        op = Operation("Word#wordcount_py", param, [])
        scheduler.compute(op)
        self._computed = True

    def print_all(self):
        assert self._computed == True, "You haven't computed wordcount"
        param = {OperationParam.list_str : self.list_name,
                "Type" : "cpp"}
        op = Operation("Word#wordcount_print_py", param, [])
        return scheduler.compute_collect(op)

    def topk(self, k):
        assert self._computed == True, "You haven't computed wordcount"
        param = {"k" : str(k),
                OperationParam.list_str : self.list_name,
                "Type" : "cpp"}
        op = Operation("Word#wordcount_topk_py", param, [])
        return scheduler.compute_collect(op)

    # def __del__(self):
    #     param = {OperationParam.list_str : self.list_name,
    #             "Type" : "cpp"}
    #     op = Operation("Word#del_py", param, [])
    #     return scheduler.compute_collect(op)
