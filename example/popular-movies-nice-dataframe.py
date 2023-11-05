# -*- coding: utf-8 -*-
"""
Created on Sun 5 23:13:00 2023

@author: Hotamul
"""

import codecs

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

DATA_DIR = "/Users/hotamul/SparkProjects/MovieRating/ml-100k/"


def load_movie_names():
    movie_names = {}
    # CHANGE THIS TO THE PATH TO YOUR u.ITEM FILE:
    with codecs.open(DATA_DIR + "u.item", "r", encoding='ISO-8859-1',
                     errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names


spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
spark.sparkContext.setLogLevel("error")

name_dict = spark.sparkContext.broadcast(load_movie_names())

# Create schema when reading u.data
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

# Load up movie data as dataframe
movies_df = spark.read.option("sep", "\t").schema(schema).csv(
    f"file://{DATA_DIR}u.data")

movie_counts = movies_df.groupBy("movie_id").count()


# Create a user-defined function to look up movie names from our broadcasted dictionary
def lookup_name(movie_id):
    return name_dict.value[movie_id]


lookup_name_udf = func.udf(lookup_name)

# Add a movieTitle column using our new udf
movies_with_names = movie_counts.withColumn("movie_title", lookup_name_udf(func.col("movie_id")))

# Sort the results
sorted_movies_with_names = movies_with_names.orderBy(func.desc("count"))

# Grab the top 10
sorted_movies_with_names.show(10, False)

# Stop the session
spark.stop()
