from pyspark.sql import SparkSession, functions
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.master("local").appName("CustomerOrdersDataFrame").getOrCreate()
spark.sparkContext.setLogLevel("error")

scheme = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("paid", FloatType(), True)
])

df = spark.read.schema(scheme).csv("file:///Users/hotamul/SparkProjects/MovieRating/example/customer-orders.csv")
df.printSchema()

customer_paid = df.select(["customer_id", "paid"]).groupBy("customer_id").agg(
    functions.round(functions.sum("paid"), 2).alias("total_paid"))

customer_paid_sorted = customer_paid.sort("total_paid")

customer_paid_sorted.show(customer_paid_sorted.count())

spark.stop()
