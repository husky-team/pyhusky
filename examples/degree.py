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

import frontend as ph

ph.env.pyhusky_start("master", 13766)

input_url = "hdfs:///datasets/graph/twitter-adj"
degree_distribution = ph.env.load(input_url) \
        .flat_map(lambda line:line.split()[2:]) \
        .map(lambda dst:(dst,1)) \
        .reduce_by_key(lambda x,y:x+y) \
        .map(lambda (k,v):(v,1)) \
        .reduce_by_key(lambda x,y:x+y) \
        .count()

print degree_distribution
