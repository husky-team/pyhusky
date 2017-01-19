class LinearRegressionModelReceiver:
    @staticmethod
    def register(receiver_map):
        receiver_map["LinearRegressionModel#LinearR_train_py"] = LinearRegressionModelReceiver.train_receiver

    @staticmethod
    def train_receiver(reply):
        res = []
        # eat dummy int64 represents the string length
        dummy = reply.load_int64()
        n_params = reply.load_int32()
        for i in xrange(n_params):
            param_v = reply.load_double()
            res.append(param_v)
        return res