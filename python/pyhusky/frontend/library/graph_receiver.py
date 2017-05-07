
class GraphReceiver:
    @staticmethod
    def register(receiver_map):
        receiver_map["Graph#pagerank_topk_py"] = GraphReceiver.topk_receiver
    
    @staticmethod
    def topk_receiver(reply):
        ret = []
        dummy = reply.load_int64() # eat dummy int64 represnts the string length
        k = reply.load_int32()
        for i in xrange(k):
            id = reply.load_int32()
            pr = reply.load_float()
            ret.append((id,pr))
        return ret
