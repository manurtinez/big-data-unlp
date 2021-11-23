from pyspark import SparkContext

sc = SparkContext('local', 'test')


rdd = sc.parallelize([1, 2, 3, 4, 5])
for i in range(2, 6):
    acc = sc.broadcast(i)
    # print("acc value " + str(acc.value))
    rdd2 = rdd.map(lambda v: v ** acc.value)
    # r = rdd2.collect()
    r = rdd2.reduce(lambda x, y: x+y)
    r = r/5
    print(r)
