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

class LogisticRegressionModel(HuskyList):
    def __init__(self, n_feature=-1):
        assert isinstance(n_feature, int)
        super(LogisticRegressionModel, self).__init__()

        self.list_name += "LogisticRgression"
        self.loaded = False
        self.trained = False
        self.param = None
        self.intercept = None

        param = {"n_feature" : str(n_feature),
                 OperationParam.list_str : self.list_name,
                 "Type" : "cpp"}
        self.pending_op = Operation("LogisticRegressionModel#LogisticR_init_py", param, [])

    def load_train_set(self, source, **kwargs):
        """Load the training dataset.

        Keyword arguments:
        source -- Source of the data. It could be a URL or a PyHuskyList.
        is_sparse -- Indicate whether it's a sparse dataset.
        format -- Indicate the type of dataset. Not applicable in the case of PyHuskyList source.
        """
        if isinstance(source, str):
            self.__load_hdfs(source, **kwargs)
        elif isinstance(source, PyHuskyList):
            self.__load_pyhlist(source, **kwargs)
        else:
            raise NotImplementedError

    def __load_hdfs(self, url, **kwargs):
        assert isinstance(url, str)
        assert not self.loaded

        if "is_sparse" not in kwargs:
            kwargs["is_sparse"] = 1
        if "format" not in kwargs:
            kwargs["format"] = "libsvm"

        self.pending_op = Operation("LogisticRegressionModel#LogisticR_load_hdfs_py", 
            {
                "url" : url,
                OperationParam.list_str : self.list_name,
                "format": kwargs["format"],
                "is_sparse": str(kwargs["is_sparse"]),
                "Type" : "cpp"
            },
            [self.pending_op]
        )
        self.loaded = True

    def __load_pyhlist(self, xy_list, **kwargs):
        assert not self.loaded

        if "is_sparse" not in kwargs:
            kwargs["is_sparse"] = 1

        if not isinstance(xy_list, PyHuskyList):
            raise NotImplementedError
        self.pending_op = Operation("LogisticRegressionModel#LogisticR_load_pyhlist_py", 
            {
                OperationParam.list_str : self.list_name,
                "is_sparse": str(kwargs["is_sparse"])
            },
            [self.pending_op]
        )
        self.loaded = True

    def train(self, n_iter=10, alpha=0.1, is_sparse=1):
        assert self.loaded
        assert isinstance(n_iter, int)
        assert isinstance(alpha, float)

        self.pending_op = Operation("LogisticRegressionModel#LogisticR_train_py",
            {
                "n_iter" : str(n_iter),
                "alpha" : str(alpha),
                OperationParam.list_str : self.list_name,
                "is_sparse": str(is_sparse),
                "Type" : "cpp"
            },
            [Operation("LogisticRegressionModel#LogisticR_init_py", {"Type" : "cpp"})] \
                if self.trained else [self.pending_op]
        )

        print self.pending_op.op_deps
        paramlist = scheduler.compute_collect(self.pending_op)
        self.param = np.array(paramlist[:-1])
        self.intercept = paramlist[-1]
        self.trained = True

    def get_param(self):
        """Return an np.array which contains the parameter (vector),
        except the intercept term.
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
