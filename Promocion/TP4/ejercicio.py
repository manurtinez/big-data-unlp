from pyspark.ml.base import Estimator, Model
from pyspark import SparkContext
from sys import argv
from pyspark.rdd import RDD
from pyspark.mllib.stat import Statistics

from pyspark.sql.dataframe import DataFrame

# splitRandom()
# Hacer join segun año y especie
# Para conseguir el error, comparar el resultado con los votos del dataset original
# train, test = rdd.randomSplit([0.8, 0.2], 1234)
# inicio = año % rango
# fin = inicio - rango

try:
    especie_param = argv[1]
except IndexError:  # No se paso el parametro necesario
    print('Debe pasarse por argumento el rango deseado')
    exit()


class MyTransformer(Model):
    __rows = []

    def addRow(self, row):
        self.__rows.append(row)

    def transform(self, dataframe: DataFrame):
        pass


class MyEstimator(Estimator):
    def fit(self, dataframe):
        return MyTransformer()


# MAIN
sc = SparkContext('local', 'myapp')

mascotas = sc.textFile('./datasets/Mascota.txt')
solicitudes = sc.textFile('./datasets/Solicitudes.txt')

# Separo cada dato
# <ID_mascota: ID, especie: string, raza: string, colorPelaje: string, edad: int, fecha de alta: AAAA-MM-DD>
rdd_mascotas = mascotas.map(lambda line: line.split('\t'))
# <ID_mascota: ID, ID_usuario: ID, motivo: string, votaci�n: int>
rdd_solicitudes = solicitudes.map(lambda line: line.split('\t'))

# Formateo cada tupla para que quede como un par clave-valor con lo que me interesa
# (id_mascota, especie)
rdd_mascotas = rdd_mascotas.map(lambda t: (t[0], (t[1], t[5])))

# (id_mascota, (id_usuario, votos))
rdd_solicitudes = rdd_solicitudes.map(lambda t: (t[0], (t[1], t[3])))

# Join por (año-especie)
rdd_join = rdd_mascotas.join(rdd_solicitudes)
rdd_join = rdd_join.map(lambda t: (t[0], (t[1][0][0], t[1][0][1], *t[1][1])))
# rdd_join.groupBy(groupFunc)
print(rdd_join.first())
# Join de datasets

# Entrenamiento modelo
# modelo = MyEstimator.fit(mascotas, solicitudes)
