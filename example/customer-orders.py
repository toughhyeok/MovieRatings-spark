from pyspark import SparkConf, SparkContext


def extract_customer_price_pairs(line):
    fields = line.split(',')
    return int(fields[0]), float(fields[2])


conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
sc = SparkContext(conf=conf)
sc.setLogLevel("error")

lines = sc.textFile("file:///Users/hotamul/SparkProjects/MovieRating/example/customer-orders.csv")
total_orders_by_customer = lines.map(extract_customer_price_pairs)
orders_by_customer = total_orders_by_customer.reduceByKey(lambda x, y: x + y)

orders_by_customer_sorted = orders_by_customer.map(lambda x: (x[1], x[0])).sortByKey()

results = orders_by_customer_sorted.collect()

for result in results:
    total_price, customer_id = result
    print("%2i:\t%.2f" % (customer_id, total_price))
