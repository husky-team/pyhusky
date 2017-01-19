import numpy as np
import frontend as ph
from frontend.library.svm import SVMModel

ph.env.pyhusky_start()

def line_parse(line):
    data = line.split()
    return ( np.array(data[:-1], dtype=float), float(data[-1]) )

def svm_hdfs():
    SVM_model = SVMModel()
    # Data can be loaded from hdfs directly
    # By providing hdfs url
    SVM_model.load_hdfs("hdfs:///datasets/classification/a9t")
    # Train the model
    SVM_model.train(n_iter = 10, alpha = 0.1)

    # Show the parameter
    # print "Vector of Parameters:"
    # print LR_model.get_param()
    # print "intercpet term: " + str(LR_model.get_intercept())

svm_hdfs()
