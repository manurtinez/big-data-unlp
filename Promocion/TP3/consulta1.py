from sys import argv
from typing import Counter
from pyspark import SparkContext

sc = SparkContext('local', 'myapp')

# Se recibe la especie por parametro
# Por ejemplo, python consultas.py "perro"
try:
    especie_param = argv[1]
except IndexError:  # No se paso el parametro necesario
    print('Debe pasarse por argumento la especie deseada')
    exit()


rdd = sc.textFile('./datasets/Mascota.txt')
# <ID_mascota: ID, especie: string, raza: string, colorPelaje: string, edad: int, fecha de alta: AAAA-MM-DD>

# Separo cada dato
rdd = rdd.map(lambda line: line.split('\t'))

# Filtro todos los que NO sean la especie deseada
rdd = rdd.filter(lambda x: x[1] == especie_param)

# Hago group by raza
rdd = rdd.groupBy(lambda x: x[2])

# Hago un map, y por cada tupla saco el maximo de ocurrencias de color (en este caso, tupla[3])
# El metodo most_common devuelve algo como [(valor, count)]. Por esto, le hago [0][0] para quedarme solo con el color (dato que me interesa)
results = rdd.map(
    lambda tupla: (tupla[0], Counter([datos[3] for datos in tupla[1]]).most_common(1)[0][0]))

# Imprimo resultados a consola
print(results.collect())

# Guardo resultados en un archivo
file = open('resultadosConsulta1.txt', 'w')
file.write(str(results.collect()))
file.close()
