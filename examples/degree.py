import frontend as ph

ph.env.pyhusky_start("master", 13766)

input_url = "hdfs:///datasets/graph/twitter-adj"
degree_distribution = ph.env.load(input_url) \
        .flat_map(lambda line:line.split()[2:]) \
        .map(lambda dst:(dst,1)) \
        .reduce_by_key(lambda x,y:x+y) \
        .map(lambda (k,v):(v,1)) \
        .reduce_by_key(lambda x,y:x+y) \
        .count()

print degree_distribution

