import frontend as ph
import frontend.env
# import frontend.graph.pagerank as pagerank
ph.env.pyhusky_start()

print """

Welcome to pyhusky...

Usages: 
use 
***
a = ph.env.load("/yuzhen/toy")
***
or 
***
a = ["1 2 3 4", "5 6 7 8", "2 3 4 5", "5 6 7 8", "3 4 32 2", "1 2"]
b = ph.env.parallelize(a)
***
to start 

Pyhusky started, use ph as pyhusky
"""
