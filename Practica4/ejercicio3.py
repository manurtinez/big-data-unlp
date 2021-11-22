from pyspark import SparkContext
from datetime import datetime
sc = SparkContext('local', 'myapp')


# datetime.strptime()
# capricornio 22/12 al 19/1

def is_capricorn(t):
    date = datetime.strptime(t[4], '%Y-%m-%d')
    return (date.month == 12 and date.day >= 22) or (date.month == 1 and date.day <= 19)


# Clientes: <ID_Cliente, nombre, apellido, DNI, fecha de nacimiento, nacionalidad>
rdd = sc.textFile('../datasets/banco/Clientes.txt')
parsed_rdd = rdd.map(lambda line: line.split('\t'))
result = parsed_rdd.filter(is_capricorn)
print(result.collect())
