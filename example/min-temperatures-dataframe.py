from pyspark.sql import functions as func, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.master("local").appName("MinTemperaturesDataFrame").getOrCreate()
spark.sparkContext.setLogLevel("error")

scheme = StructType([
    StructField("station_id", StringType(), True),
    StructField("date", IntegerType(), True),
    StructField("measure_type", StringType(), True),
    StructField("temperature", FloatType(), True)
])

df = spark.read.schema(scheme).csv("file:///Users/hotamul/SparkProjects/MovieRating/example/1800.csv")
df.printSchema()

min_temps = df.filter(df.measure_type == "TMIN")

# Select Only stationID and temperature
station_temps = min_temps.select(["station_id", "temperature"])

# Aggregate to find minimum temperature for every station
min_temps_by_station = station_temps.groupBy("station_id").min("temperature")
min_temps_by_station.show()

# Convert temperature to fahrenheit and sort the dataset
min_temps_station_F = min_temps_by_station.withColumn(
    "temperature",
    func.round(
        func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0,
        2
    )).select("station_id", "temperature").sort("temperature")

results = min_temps_station_F.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))

spark.stop()
