import frontend as ph
import time

ph.env.pyhusky_start("master", 10086)

# collect

# test-1-load-collect
# a = [1, 2, 3]
# degree_distribution = ph.env.parallelize(a) \
#         .collect()
# print degree_distribution

# test-2-map-collect
# a = [1, 2, 1, 2, 3, 4]
# degree_distribution = ph.env.parallelize(a) \
# 		.map(lambda dst:(dst, 1)) \
# 		.collect()

#print degree_distribution

# test-3-flat-map-collect
# a = [1, 2, 1, 2, 3, 4]
# degree_distribution = ph.env.parallelize(a) \
# 		.flat_map(lambda dst:(dst, 1)) \
# 		.collect()

# print degree_distribution

# test-3-concat-collect
# a = [1, 2, 3]
# b = [4, 5, 6]
# b_py_list = ph.env.parallelize(b)

# degree_distribution = ph.env.parallelize(a) \
# 		.concat(b_py_list) \
# 		.collect()

# print degree_distribution

# test-4-filter-collect
# a = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# degree_distribution = ph.env.parallelize(a) \
# 		.filter(lambda x: x > 5) \
# 		.collect()

# print degree_distribution

#test-5-reduce-by-key-collect
# a = [1, 2, 3, 4, 5, 5, 7, 8, 9, 9, 9, 10]

# degree_distribution = ph.env.parallelize(a) \
# 		.map(lambda x: (x, 1)) \
# 		.reduce_by_key(lambda x,y:x+y) \
# 		.collect()

# print degree_distribution

# test-6-group-by-key
# a = [1, 2, 3, 3, 3, 5, 5, 6]

# degree_distribution = ph.env.parallelize(a) \
# 		.map(lambda x: (x, 1)) \
# 		.group_by_key() \
# 		.collect()

# print degree_distribution

# test-7-count_by_key_collect
# a = [1, 2, 3, 3, 3, 5, 5, 6]

# degree_distribution = ph.env.parallelize(a) \
# 		.map(lambda x: (x, 1)) \
# 		.count_by_key() \
# 		.collect()

# print degree_distribution

#test-8-difference_collect
# a = [1, 2, 3, 3, 3, 5, 5, 6]
# b = [1, 2]

# b = ph.env.parallelize(b) \
# 	.map(lambda x: (x, 1))

# degree_distribution = ph.env.parallelize(a) \
# 		.map(lambda x: (x, 1)) \
# 		.difference(b) \
# 		.collect()

# print degree_distribution

#test-9-distinct-collect
# a = [1, 2, 3, 3, 3, 5, 5, 6]

# degree_distribution = ph.env.parallelize(a) \
# 		.map(lambda x: (x, 1)) \
# 		.distinct() \
# 		.collect()

# print degree_distribution


#test-10-reduce
# a = [1, 2, 3, 3, 3, 5, 5, 6]

# degree_distribution = ph.env.parallelize(a) \
# 		.reduce(lambda x,y:x+y)

# print degree_distribution

# count

# test-11-load-count
# a = [1, 2, 3, 1, 2]
# degree_distribution = ph.env.parallelize(a) \
# 		.map(lambda x: (x, 1)) \
# 		.reduce_by_key(lambda x,y:x+y) \
#         .collect()
# print degree_distribution

# test-12-map-count
# a = [1, 2, 1, 2, 3, 4]
# degree_distribution = ph.env.parallelize(a) \
# 		.map(lambda dst:(dst, 1)) \
# 		.count()

# print degree_distribution

# test-13-load

# input_url = "hdfs:////yuzhen/toy"
# degree_distribution = ph.env.load(input_url) \
#        .flat_map(lambda line:line.split()[2:]) \
 #       .map(lambda dst:(dst,1)) \
  #      .reduce_by_key(lambda x,y:x+y) \
   #     .map(lambda (k,v):(v,1)) \
    #    .reduce_by_key(lambda x,y:x+y) \
     #   .collect()

# print degree_distribution

# input_url = "/datasets/graph/amazon-adj"
# degree_distribution = ph.env.load(input_url) \
#        .flat_map(lambda line:line.split()[2:]) \
#        .map(lambda dst:(dst,1)) \
#        .reduce_by_key(lambda x,y:x+y) \
#        .map(lambda (k,v):(v,1)) \
#        .reduce_by_key(lambda x,y:x+y) \
#        .collect()

# print degree_distribution

# input_url = "/datasets/graph/twitter-adj"
# degree_distribution = ph.env.load(input_url) \
#        .flat_map(lambda line:line.split()[2:]) \
#        .map(lambda dst:(dst,1)) \
#        .reduce_by_key(lambda x,y:x+y) \
#        .map(lambda (k,v):(v,1)) \
#        .reduce_by_key(lambda x,y:x+y) \
#        .collect()

# print degree_distribution

# input_url = "/tmp/toy.txt"

# degree_distribution = ph.env.load(input_url) \
#        .collect()

# print degree_distribution

# input_url = "hdfs:///datasets/graph/amazon-adj"
# input_url = "hdfs:///datasets/graph/twitter-adj"
# # input_url = "hdfs:////yuzhen/toy"
# # input_url = "hdfs:///datasets/graph/livej-adj-8m"
# start = time.time()
# degree_distribution = ph.env.load(input_url) \
#        .flat_map(lambda line:line.split()[2:]) \
#        .map(lambda dst:(dst,1)) \
#        .reduce_by_key(lambda x,y:x+y) \
# 	   .count()

# print time.time() - start  
# print degree_distribution

# input_url = "nfs:///data/yuying/project/h3/tmp/toy"
# input_url = "hdfs:////yuzhen/toy"
input_url = "hdfs:///datasets/graph/amazon-adj"
start = time.time()
degree_distribution = ph.env.load(input_url) \
        .flat_map(lambda line:line.split()[2:]) \
	.map(lambda dst:(dst,1)) \
        .reduce_by_key(lambda x,y:x+y) \
        .collect()

print degree_distribution 
