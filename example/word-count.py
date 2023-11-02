import re

from pyspark import SparkConf, SparkContext


def normalization_words(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)
sc.setLogLevel("error")

lines = sc.textFile("file:///Users/hotamul/SparkProjects/MovieRating/example/Book")
words = lines.flatMap(normalization_words)
words_counts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
words_counts_sorted = words_counts.map(lambda x: (x[1], x[0])).sortByKey()

results = words_counts_sorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if word:
        print(word.decode() + ":\t\t" + count)
