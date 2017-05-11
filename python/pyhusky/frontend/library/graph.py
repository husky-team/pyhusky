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

class Graph(HuskyList):
    def __init__(self):
        super(Graph, self).__init__()
        self.list_name += "PRVertex"
        self._loaded = False
        self._computed = False

    def load_edgelist_phlist(self, edgelist):
        assert self._loaded == False, "The graph was already loaded"
        assert isinstance(edgelist, PyHuskyList)
        param = {OperationParam.list_str : self.list_name}
        op = Operation("Graph#load_edgelist_phlist_py", param, [edgelist.pending_op])
        scheduler.compute(op)
        self._loaded = True

    def load_adjlist_hdfs(self, url):
        assert type(url) is str
        assert self._loaded == False, "The graph was already loaded"
        param = {OperationParam.list_str : self.list_name,
                "url" : url,
                "Type" : "cpp"}
        op = Operation("Graph#load_adjlist_hdfs_py", param, []);
        scheduler.compute(op)
        self._loaded = True

    def compute_pagerank(self, iter):
        assert self._loaded == True, "The graph is not loaded"
        param = {"iter" : str(iter), 
                 OperationParam.list_str : self.list_name,
                 "Type" : "cpp"}
        op = Operation("Graph#pagerank_py", param, [])
        scheduler.compute(op)
        self._computed = True

    def topk_pagerank(self, k):
        assert self._computed == True, "You haven't computed Pagerank"
        param = {"k" : str(k), 
                 OperationParam.list_str : self.list_name,
                 "Type" : "cpp"}
        op = Operation("Graph#pagerank_topk_py", param, [])
        topk_list = scheduler.compute_collect(op)
        return topk_list

