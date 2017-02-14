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

from pyhusky.common.operation import Operation, OperationParam
from pyhusky.frontend import scheduler
from pyhusky.frontend.huskylist import HuskyList
from pyhusky.frontend.huskylist import PyHuskyList

class LinearRegressionModel(HuskyList):
    def __init__(self, n_feature=-1):
        assert isinstance(n_feature, int)
        super(LinearRegressionModel, self).__init__()

        self.list_name += "LinearRgression"
        self.loaded = False
        self.trained = False
        self.param = None
        self.intercept = None

        param = {"n_feature" : str(n_feature),
                 OperationParam.list_str : self.list_name,
                 "Type" : "cpp"}
        op = Operation("LinearRegressionModel#LinearR_init_py", param, [])
        scheduler.compute(op)

    def load_hdfs(self, url, is_sparse=0, fmat="tsv"):
        assert isinstance(url, str)
        assert not self.loaded

        param = {"url" : url,
                 OperationParam.list_str : self.list_name,
                 "is_sparse": str(is_sparse),
                 "format": fmat,
                 "Type" : "cpp"}
        op = Operation("LinearRegressionModel#LinearR_load_hdfs_py", param, [])
        scheduler.compute(op)
        self.loaded = True

    def load_pyhlist(self, xy_list, is_sparse=1):
        assert not self.loaded

        if isinstance(xy_list, PyHuskyList):
            param = {OperationParam.list_str : self.list_name,
                     "is_sparse": str(is_sparse)}
            self.pending_op = Operation("LinearRegressionModel#LinearR_load_pyhlist_py", param, [xy_list.pending_op])
            scheduler.compute(self.pending_op)
            self.loaded = True
        else:
            return NotImplemented

    def train(self, n_iter=10, alpha=0.1, is_sparse=1):
        assert self.loaded
        assert isinstance(n_iter, int)
        assert isinstance(alpha, float)

        param = {"n_iter" : str(n_iter),
                 "alpha" : str(alpha),
                 OperationParam.list_str : self.list_name,
                 "is_sparse": str(is_sparse),
                 "Type" : "cpp"}
        op = Operation("LinearRegressionModel#LinearR_train_py", param, [])
        paramlist = scheduler.compute_collect(op)
        self.param = np.array(paramlist[:-1])
        self.intercept = paramlist[-1]
        self.loaded = False
        self.trained = True

    def get_param(self):
        """
        Returns:
            An np.array which contains the parameter (vector)
            But except intercept term
        """
        assert self.trained

        return self.param

    def get_intercept(self):
        assert self.trained

        return self.intercept

    def predict(self, X):
        assert self.trained
        assert hasattr(X, '__iter__')

        return np.dot(np.array(X), self.param) + self.intercept
