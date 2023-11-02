from pyspark.sql import SparkSession, functions

spark = SparkSession.builder.master("local").appName("FriendsByAgeDataFrame").getOrCreate()
spark.sparkContext.setLogLevel("error")

people = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("file:///Users/hotamul/SparkProjects/MovieRating/example/fakefriends-header.csv")

totals_by_age = people.select(["age", "friends"])
totals_by_age.groupBy("age").avg("friends").sort("age").show()

totals_by_age.groupBy("age").agg(functions.round(functions.avg("friends"), 2)
                                 .alias("friends_avg")).sort("age").show()
print(totals_by_age)

spark.stop()
