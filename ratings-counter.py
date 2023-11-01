import collections

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///Users/hotamul/SparkProjects/MovieRating/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
results = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(results.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
