from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

DATA_DIR = "/Users/hotamul/SparkProjects/MovieRating/example"

spark = SparkSession.builder.appName("MostObscureSuperheroes").getOrCreate()
spark.sparkContext.setLogLevel("error")

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv(f"file://{DATA_DIR}/Marvel-names")

lines = spark.read.text(f"file://{DATA_DIR}/Marvel-graph")

# Small tweak vs. what's shown in the video: we trim whitespace from each line as this
# could throw the counts off by one.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

min_connection_count = connections.agg(func.min("connections")).first()[0]

min_connections = connections.filter(func.col("connections") == min_connection_count)

min_connections_with_names = min_connections.join(names, "id")

print("The following characters have only " + str(min_connection_count) + " connection(s):")

min_connections_with_names.select("name").show()
