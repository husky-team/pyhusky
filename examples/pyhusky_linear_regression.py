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

from pyhusky.frontend.library.linear_regression import LinearRegressionModel
import pyhusky.frontend as ph

ph.env.pyhusky_start()

def line_parse(line):
    data = line.split()
    return ( np.array(data[:-1], dtype=float), float(data[-1]) )

def linear_regresion():
    # Load data into PyHuskyList
    train_list = ph.env.load("hdfs:///datasets/regression/MSD/test").map(line_parse)
    
    # Train the model
    LR_model = LinearRegressionModel()
    LR_model.load_pyhlist(train_list)
    LR_model.train(n_iter = 10, alpha = 0.1)

    # Show the parameter
    # print "Vector of Parameters:"
    # print LR_model.get_param()
    # print "intercpet term: " + str(LR_model.get_intercept())

    # Show the average error
    # n_train_sample = train_list.count()
    # truth_and_predict = train_list.map(lambda p: (p[1], LR_model.predict(p[0])))
    # average_error = truth_and_predict.map(lambda x: (x[0]-x[1])**2).reduce(lambda x, y: x+y)/n_train_sample
    # print "average_error = " + str(average_error)

def linear_regresion_hdfs():
    LR_model = LinearRegressionModel()
    # Data can be loaded from hdfs directly
    # By providing hdfs url
    LR_model.load_hdfs("hdfs:///datasets/regression/MSD/test")
    # Train the model
    LR_model.train(n_iter = 10, alpha = 0.1)

    # Show the parameter
    # print "Vector of Parameters:"
    # print LR_model.get_param()
    # print "intercpet term: " + str(LR_model.get_intercept())

# linear_regresion()
linear_regresion_hdfs()
