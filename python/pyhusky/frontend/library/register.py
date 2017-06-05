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

from pyhusky.frontend.library.linear_regression_receiver import LinearRegressionModelReceiver
from pyhusky.frontend.library.logistic_regression_receiver import LogisticRegressionModelReceiver
from pyhusky.frontend.library.svm_receiver import SVMReceiver
from pyhusky.frontend.library.word_receiver import WordReceiver 
from pyhusky.frontend.library.graph_receiver import GraphReceiver

def register(receiver_map):
    LinearRegressionModelReceiver.register(receiver_map)
    LogisticRegressionModelReceiver.register(receiver_map)
    WordReceiver.register(receiver_map)
    GraphReceiver.register(receiver_map)
    SVMReceiver.register(receiver_map)
