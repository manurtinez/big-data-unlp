from sys import argv

from pyspark import Row, SparkContext
from pyspark.ml.base import Estimator, Model
from pyspark.sql import SQLContext, Window
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import avg, round, sum, array_contains, array

# splitRandom()
# Hacer join segun año y especie
# Para conseguir el error, comparar el resultado con los votos del dataset original
# train, test = rdd.randomSplit([0.8, 0.2], 1234)
# inicio = año % rango
# fin = inicio - rango

try:
    rango = argv[1]
except IndexError:  # No se paso el parametro necesario
    print('Debe pasarse por argumento el rango deseado')
    exit()

# SPARK CONTEXT Y SQL CONTEXT
sc = SparkContext('local', 'myapp')
sqlc = SQLContext(sc)


class MyTransformer(Model):

    def __init__(self, params):
        super().__init__()
        self._params = params

    def _transform(self, dataframe: DataFrame):
        # Condiciones del join: [ fecha alta esta dentro de rango && especie == especie ]
        cond = [array_contains(self._params.rango, dataframe.fecha_alta),
                self._params.especie == dataframe.especie]

        # Alias first y sec para tablas.
        # Select fecha, especie, votos y promedio
        return dataframe.alias("df") \
            .join(self._params.alias("params"), cond, "left") \
            .select("df.fecha_alta", "df.especie", "df.votos", "params.promedio") \
            .na.fill("None")  # Filas sin info se llenan con None


class MyEstimator(Estimator):
    def _fit(self, dataframe):
        dataframe = dataframe.withColumn("rango", array([dataframe.fecha_alta+i for i in range(int(rango))])) \
            .withColumn("promedio", round(avg("votos").over(Window.partitionBy("rango", "especie")))) \
            .select("rango", "especie", "promedio").dropDuplicates()

        print("------ DATAFRAME DE ESTIMATOR ------")
        dataframe.show()
        return MyTransformer(dataframe)


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
    id_mascota=line[0], especie=line[1], fecha_alta=int(line[5][:4])))  # Solo me interesa el año en la fecha
rdd_solicitudes = rdd_solicitudes.map(
    lambda line: Row(id_mascota=line[0], votos=line[3]))

# Creo dataframes
mascotas_df = sqlc.createDataFrame(rdd_mascotas)
solicitudes_df = sqlc.createDataFrame(rdd_solicitudes)

# Hacer el join de dataframes
join_df = mascotas_df.join(solicitudes_df, 'id_mascota')

# Separar 80% para train y 20% para test (para sacar el error)
train, test = join_df.randomSplit([0.8, 0.2])

# Entrenamiento modelo
estimator = MyEstimator()
model = estimator.fit(train)

# Transformar modelo de test
results = model.transform(test)

# Mostrar resultado del transformer con columna promedio
print("------ DATAFRAME DE PREDICCION -------")
results.select("especie", "fecha_alta", "promedio").dropDuplicates().show()

results = results.withColumn(
    "diferencia", (results.votos - results.promedio) ** 2)
error = results.agg(sum(results.diferencia)).collect()
print("------ ERROR ------\n", str(error[0][0] / results.count()))
