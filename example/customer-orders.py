from pyspark import SparkConf, SparkContext


def extract_customer_price_pairs(line):
    fields = line.split(',')
    return int(fields[0]), float(fields[2])


conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
sc = SparkContext(conf=conf)
sc.setLogLevel("error")

lines = sc.textFile("file:///Users/hotamul/SparkProjects/MovieRating/example/customer-orders.csv")
total_orders_by_customer = lines.map(extract_customer_price_pairs)
orders_by_customer = total_orders_by_customer.reduceByKey(lambda x, y: x + y).sortByKey()

results = orders_by_customer.collect()

for result in results:
    print("%2i:\t%.2f" % result)
