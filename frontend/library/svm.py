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

from frontend.huskylist import HuskyList
from frontend.huskylist import PyHuskyList
from frontend.operation import Operation, OperationParam
from frontend import scheduler

class SVMModel(HuskyList):
    def __init__(self, n_feature = -1):
        assert type(n_feature) is int

        super(SVMModel, self).__init__()
        self.list_name += "SVM"
        self._loaded = False
        self._trained = False

        param = {"n_feature" : str(n_feature),
                OperationParam.list_str : self.list_name, 
                "Type" : "cpp"}
        op = Operation("SVMModel#SVM_init_py", param, [])
        scheduler.compute(op)

    def load_hdfs(self, url):
        assert type(url) is str
        assert self._loaded == False

        param = {"url" : url,
                OperationParam.list_str : self.list_name, 
                "Type" : "cpp"}
        op = Operation("SVMModel#SVM_load_hdfs_py", param, []);
        scheduler.compute(op)
        self._loaded = True

    def load_pyhlist(self, xy_list):
        assert self._loaded == False

        if isinstance(xy_list, PyHuskyList):
            param = {OperationParam.list_str : self.list_name}
            self.pending_op = Operation("SVMModel#SVM_load_pyhlist_py",
                    param,
                    [xy_list.pending_op])
            scheduler.compute(self.pending_op)
            self._loaded = True
        else:
            return NotImplemented

    def train(self, n_iter=10, alpha=0.1):
        assert self._loaded is True
        assert type(n_iter) is int
        assert type(alpha) is float
        
        param = {"n_iter" : str(n_iter),
                "alpha" : str(alpha),
                OperationParam.list_str : self.list_name,
                "Type" : "cpp"}
        op = Operation("SVMModel#SVM_train_py", param, []);
        param_list = scheduler.compute_collect(op)
        self.param_ = np.array(param_list[:-1])
        self.intercept_ = param_list[-1]
        self._loaded = False
        self._trained = True

    def get_param(self):
        """
        Returns:
            An np.array which contains the parameter (vector)
            But except intercpet term
        """
        assert self._trained == True

        return self.param_

    def get_intercept(self):
        assert self._trained == True

        return self.intercept_

    def predict(self, X):
        assert self._trained == True
        assert hasattr(X, '__iter__')

        return np.dot(np.array(X), self.param_) + self.intercept_
        
