import types

class OperationParam:
    data_str = "data"
    url_str = "url"
    lambda_str = "lambda"
    list_str = "list_name"

class Operation:
    def __init__(self, op_name, op_param, op_deps=[]):
        self.op_name = op_name
        # assert isinstance(op_param, types.ListType)
        # self.op_param = op_param
        assert isinstance(op_param, types.DictType)
        self.op_param = op_param
        self.op_deps = [dep for dep in op_deps if dep is not None]
        self.is_materialized = False
    def __repr__(self):
        return "<"+self.op_name+">"
        # return "<"+self.op_name+">"+" ".join(str(x) for x in self.op_param)
