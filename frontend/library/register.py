# from frontend.library.graphreceiver import GraphReceiver
# from frontend.library.wordreceiver import WordReceiver
from frontend.library.linear_regression_receiver import LinearRegressionModelReceiver
from frontend.library.logistic_regression_receiver import LogisticRegressionModelReceiver
# from frontend.library.spca_receiver import SPCAReceiver
# from frontend.library.tfidf_receiver import TFIDFReceiver
# from frontend.library.bm25receiver import BM25Receiver
from frontend.library.svm_receiver import SVMReceiver

def register(receiver_map):
    # GraphReceiver.register(receiver_map)
    # WordReceiver.register(receiver_map)
    LinearRegressionModelReceiver.register(receiver_map)
    # SVMReceiver.register(receiver_map)
    LogisticRegressionModelReceiver.register(receiver_map)
    # SPCAReceiver.register(receiver_map)
    # TFIDFReceiver.register(receiver_map)
    # BM25Receiver.register(receiver_map)
