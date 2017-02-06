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

import numpy as np
import frontend as ph
from frontend.library.svm import SVMModel

ph.env.pyhusky_start()

def line_parse(line):
    data = line.split()
    return ( np.array(data[:-1], dtype=float), float(data[-1]) )

def svm_hdfs():
    SVM_model = SVMModel()
    # Data can be loaded from hdfs directly
    # By providing hdfs url
    SVM_model.load_hdfs("hdfs:///datasets/classification/a9t")
    # Train the model
    SVM_model.train(n_iter = 10, alpha = 0.1)

    # Show the parameter
    # print "Vector of Parameters:"
    # print LR_model.get_param()
    # print "intercpet term: " + str(LR_model.get_intercept())

svm_hdfs()
