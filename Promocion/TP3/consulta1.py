from sys import argv
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

# Hago que la clave sea (raza, color) y el value 1 para hacer un count
rdd = rdd.map(lambda t: ((t[2], t[3]), 1))

# Se hace count de cada par raza-color
rdd = rdd.reduceByKey(lambda t1, t2: t1 + t2)

# Hago que la nueva key sea raza y el nuevo value sea (color, count)
rdd = rdd.map(lambda t: (t[0][0], (t[0][1], t[1])))

# Me quedo con el color maximo de cada raza
# t1 y t2: (color, count)
rdd = rdd.reduceByKey(lambda t1, t2: t1 if t1[1] > t2[1] else t2)

# Formateo los resultados
results = rdd.map(lambda t: (t[0], t[1][0])).collect()

# Imprimo resultados a consola
for tupla in results:
    print(tupla)

# Guardo resultados en un archivo
with open('resultadosConsulta1.txt', 'w') as file:
    file.write('Color mas predominante para cada raza de la especie deseada:\n')
    for tupla in results:
        file.write(str(tupla) + '\n')
    file.close()
