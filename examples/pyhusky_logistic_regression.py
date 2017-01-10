import numpy as np
import frontend as ph
from frontend.library.logistic_regression import LogisticRegressionModel

ph.env.pyhusky_start()

def line_parse(line):
    data = line.split()
    return ( np.array(data[:-1], dtype=float), float(data[-1]) )

def logistic_regresion_hdfs():
    LogisticR_model = LogisticRegressionModel()
    # Data can be loaded from hdfs directly
    # By providing hdfs url
    LogisticR_model.load_hdfs("hdfs:///datasets/classification/a9t")
    # Train the model
    LogisticR_model.train(n_iter = 10, alpha = 0.1)

    # Show the parameter
    # print "Vector of Parameters:"
    # print LR_model.get_param()
    # print "intercpet term: " + str(LR_model.get_intercept())

logistic_regresion_hdfs()
