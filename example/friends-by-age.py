from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)
sc.setLogLevel("error")


def parse_line(line):
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)


lines = sc.textFile("file:///Users/hotamul/SparkProjects/MovieRating/example/fakefriends.csv")
rdd = lines.map(parse_line)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: int(x[0] / x[1]))
results = averagesByAge.collect()
results.sort(key=lambda x: x[0])
for result in results:
    print(result)
