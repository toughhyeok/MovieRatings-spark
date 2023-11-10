import codecs
import sys

from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType


def load_movie_names():
    movie_names = {}
    # CHANGE THIS TO THE PATH TO YOUR u.ITEM FILE:
    with codecs.open("/Users/hotamul/SparkProjects/MovieRating/ml-100k/u.item", "r", encoding='ISO-8859-1',
                     errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names


spark = SparkSession.builder.appName("ALSExample").getOrCreate()
spark.sparkContext.setLogLevel("error")

movies_schema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)])

names = load_movie_names()

ratings = spark.read.option("sep", "\t").schema(movies_schema) \
    .csv("file:///Users/hotamul/SparkProjects/MovieRating/ml-100k/u.data")

print("Training recommendation model...")

als = ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userID").setItemCol("movieID") \
    .setRatingCol("rating")

model = als.fit(ratings)

# Manually construct a dataframe of the user ID's we want recs for
user_id = int(sys.argv[1])
user_schema = StructType([StructField("userID", IntegerType(), True)])
users = spark.createDataFrame([[user_id, ]], user_schema)

recommendations = model.recommendForUserSubset(users, 10).collect()

print("Top 10 recommendations for user ID " + str(user_id))

for user_recs in recommendations:
    my_recs = user_recs[1]  # userRecs is (userID, [Row(movieId, rating), Row(movieID, rating)...])
    for rec in my_recs:  # my Recs is just the column of recs for the user
        movie = rec[0]  # For each rec in the list, extract the movie ID and rating
        rating = rec[1]
        movie_name = names[movie]
        print(movie_name + str(rating))
