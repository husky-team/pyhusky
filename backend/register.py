import backend.library.functional as functional
# import backend.library.graph as graph
# import backend.library.word as word
import backend.library.linear_regression as LinearR
import backend.library.logistic_regression as LogisticR
# import backend.library.spca as SPCA
# import backend.library.tfidf as tfidf
# import backend.library.bm25 as BM25
import backend.library.svm as SVM

def register_func():
    # register
    # functional
    functional.register_all()
    LinearR.register_all()
    # SVM.register_all()
    LogisticR.register_all()
