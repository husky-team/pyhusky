
class WordReceiver:
    @staticmethod
    def register(receiver_map):
        receiver_map["Word#wordcount_topk_py"] = WordReceiver.wordcount_topk_receiver
        receiver_map["Word#wordcount_print_py"] = WordReceiver.wordcount_print_receiver

    @staticmethod
    def wordcount_topk_receiver(reply):
        ret = []
        dummy = reply.load_int64() # eat dummy int64 represnts the string length
        k = reply.load_int32()
        for i in xrange(k):
            word = reply.load_str()
            count = reply.load_int32()
            ret.append((word, count))
        return ret

    @staticmethod
    def wordcount_print_receiver(reply):
        ret = []
        dummy = reply.load_int64() # eat dummy int64 represnts the string length
        k = reply.load_int32()
        for i in xrange(k):
            word = reply.load_str()
            count = reply.load_int32()
            ret.append((word, count))
        return ret
