from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalSpentByCustomer")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(',')
    id = int(fields[0])
    coast = float(fields[2])
    return id, coast


lines = sc.textFile("customer-orders.csv")
rdd = lines.map(parseLine)
totalsByCustomerId = rdd.reduceByKey(lambda x, y: (x + y))
totalsByCustomerIdSorted = totalsByCustomerId.map(lambda x: (x[1], x[0])).sortByKey()
results = totalsByCustomerIdSorted.collect()
for result in results:
    print(result[1], "{:.2f}".format(result[0]))
