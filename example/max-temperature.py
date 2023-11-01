from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
sc = SparkContext(conf=conf)
sc.setLogLevel("error")


def parse_line(line):
    fields = line.split(',')
    return fields[0], fields[2], float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0


lines = sc.textFile("file:///Users/hotamul/SparkProjects/MovieRating/example/1800.csv")
parsed_lines = lines.map(parse_line)
max_temps = parsed_lines.filter(lambda x: "TMAX" in x[1])
stations_temps = max_temps.map(lambda x: (x[0], x[2]))
max_temps = stations_temps.reduceByKey(lambda x, y: max(x, y))
results = max_temps.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
