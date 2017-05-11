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

import pyhusky.frontend as ph
from pyhusky.frontend.library.graph import Graph

ph.env.pyhusky_start("master", 32441)

edges = [(1,2),(2,3),(3,4),(4,5),(5,1),(1,3),(4,3)]
edgelist = ph.env.parallelize(edges)

graph = Graph()
graph.load_edgelist_phlist(edgelist)
graph.compute_pagerank(iter=10)
print graph.topk_pagerank(2)
