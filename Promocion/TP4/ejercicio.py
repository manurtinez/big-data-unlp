from pyspark.ml.base import Estimator, Model
from pyspark import SparkContext, Row
from sys import argv

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SQLContext

# splitRandom()
# Hacer join segun año y especie
# Para conseguir el error, comparar el resultado con los votos del dataset original
# train, test = rdd.randomSplit([0.8, 0.2], 1234)
# inicio = año % rango
# fin = inicio - rango

# try:
#     rango = argv[1]
# except IndexError:  # No se paso el parametro necesario
#     print('Debe pasarse por argumento el rango deseado')
#     exit()


class MyTransformer(Model):
    __rows = []

    def addRow(self, row):
        self.__rows.append(row)

    def transform(self, dataframe: DataFrame):
        pass


class MyEstimator(Estimator):
    def fit(self, dataframe):
        return MyTransformer()


sc = SparkContext('local', 'myapp')
sqlc = SQLContext(sc)

# MAIN

mascotas = sc.textFile('./datasets/Mascota.txt')
solicitudes = sc.textFile('./datasets/Solicitudes.txt')

# Separo cada dato
# <ID_mascota: ID, especie: string, raza: string, colorPelaje: string, edad: int, fecha de alta: AAAA-MM-DD>
rdd_mascotas = mascotas.map(lambda line: line.split('\t'))
# <ID_mascota: ID, ID_usuario: ID, motivo: string, votaci�n: int>
rdd_solicitudes = solicitudes.map(lambda line: line.split('\t'))

# Transformo a Row pasa usar SQL
rdd_mascotas = rdd_mascotas.map(lambda line: Row(
    id_mascota=line[0], fecha_alta=line[5]))
rdd_solicitudes = rdd_solicitudes.map(
    lambda line: Row(id_mascota=line[0], votos=line[3]))

# Creo dataframes
mascotas_df = sqlc.createDataFrame(rdd_mascotas)
solicitudes_df = sqlc.createDataFrame(rdd_solicitudes)

join_df = mascotas_df.join(solicitudes_df, 'id_mascota')
print(join_df.first())

# # Separo cada dato
# # <ID_mascota: ID, especie: string, raza: string, colorPelaje: string, edad: int, fecha de alta: AAAA-MM-DD>
# rdd_mascotas = mascotas.map(lambda line: line.split('\t'))
# # <ID_mascota: ID, ID_usuario: ID, motivo: string, votaci�n: int>
# rdd_solicitudes = solicitudes.map(lambda line: line.split('\t'))

# # Formateo cada tupla para que quede como un par clave-valor con lo que me interesa
# # (id_mascota, especie)
# rdd_mascotas = rdd_mascotas.map(lambda t: (t[0], (t[1], t[5])))

# # (id_mascota, (id_usuario, votos))
# rdd_solicitudes = rdd_solicitudes.map(lambda t: (t[0], (t[1], t[3])))

# # Join por (año-especie)
# rdd_join = rdd_mascotas.join(rdd_solicitudes)
# rdd_join = rdd_join.map(lambda t: (t[0], (t[1][0][0], t[1][0][1], *t[1][1])))
# # rdd_join.groupBy(groupFunc)
# print(rdd_join.first())
# Entrenamiento modelo
# modelo = MyEstimator.fit(mascotas, solicitudes)
