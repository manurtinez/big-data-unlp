from pyspark import SparkContext
sc = SparkContext('local', 'myapp')


def formatLines(raw_line):
    numbers = raw_line.split(' ')
    return (int(numbers[0]), int(numbers[1]))


rdd = sc.textFile('./datasetpunto1.txt')
rdd = rdd.map(formatLines)

# a
# res = rdd.map(lambda t: (int(t[0]) + int(t[1]) * 2))
# print(res.first())

# b
# res = rdd.filter(lambda t: t[0] >= t[1])
# print(res.take(3))

# c
# res = rdd.map(lambda t: (t[0], t[1], t[0] / t[1]))
# res = res.filter(lambda t: t[2] < 0.5)
# res = res.reduce(lambda t1, t2: t1 if t1[2] < t2[2] else t2)
# print(res)

# d
# r1 = rdd.map(lambda t: t[0])
# r2 = rdd.map(lambda t: t[1])
# r1 = r1.distinct()  # elimina 32 repetidos
# r2 = r2.distinct()  # elimina 45, 18
# res = r2.union(r1)
# print(res.collect())
