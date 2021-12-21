from pyspark import SparkContext

sc = SparkContext('local', 'myapp')


def findMax(res, ori):
    """
    Esta funcion sirve para comparar el maximo parcial que se va llevando en el aggregate con cada tupla
    """
    if res == ((), (), ()):
        return (ori, ori, ori)
    new_result = [res[0], res[1], res[2]]
    if int(ori[2]) > int(res[0][2]):
        # Se mueven hacia abajo 1er y 2do puesto
        new_result[2] = new_result[1]
        new_result[1] = new_result[0]
        new_result[0] = ori
    elif int(ori[2]) > int(res[1][2]):
        # Se mueve hacia abajo el 2do puesto
        new_result[2] = new_result[1]
        new_result[1] = ori
    elif int(ori[2]) > int(res[2][2]):
        new_result[2] = ori
    return tuple(new_result)


# <ID_mascota: ID, especie: string, raza: string, colorPelaje: string, edad: int, fecha de alta: AAAA-MM-DD>
rdd_mascotas = sc.textFile('./datasets/Mascota.txt')
# <ID_mascota: ID, ID_usuario: ID, motivo: string, votaci�n: int>
rdd_solicitudes = sc.textFile('./datasets/Solicitudes.txt')

# Separo cada dato
rdd_mascotas = rdd_mascotas.map(lambda line: line.split('\t'))
rdd_solicitudes = rdd_solicitudes.map(lambda line: line.split('\t'))

# Formateo cada tupla para que quede como un par clave-valor con lo que me interesa
# (id_mascota, especie)
rdd_mascotas = rdd_mascotas.map(lambda t: (t[0], t[1]))

# (id_mascota, (id_usuario, votos))
rdd_solicitudes = rdd_solicitudes.map(lambda t: (t[0], (t[1], t[3])))

# Hago un join de las 2 rdd
rdd_join = rdd_mascotas.join(rdd_solicitudes)

# Cambio la key a ESPECIE y "aplano" la tupla de valor luego del join, para hacer un reduceByKey
rdd_join = rdd_join.map(lambda t: (t[1][0], (t[0], t[1][1][0], t[1][1][1])))

# Reduzco
# (id, (id, votos)) entrada
# [(id, id, votos), (id, id, votos),(id, id, votos)] salida
results = rdd_join.aggregateByKey(
    ((), (), ()),
    findMax,
    lambda result1, result2: sorted(
        result1 + result2, key=lambda item: int(item[2]), reverse=True)[:3]  # Ordena la combinacion de los 2 resultados y saca el top 3
).collect()

# Imprimo resultados a consola
print(results)

# Guardo resultados a un archivo para facil lectura
with open('resultadosTopVotos.txt', 'w') as file:
    file.write('Tres pares mascota-cliente con mas votos para c/ especie\n')
    for tup_especie in results:
        file.write('---especie: '+f'{tup_especie[0]}---\n')
        for data in tup_especie[1]:
            file.write(
                f'id mascota: {data[0]}, id usuario: {data[1]}, votos: {data[2]}\n')
    file.close()


# !! ACA ABAJO DEJO LA FORMA INEFICIENTE PERO FUNCIONAL DE RESOLVER LA CONSULTA

# Ordeno rdd por votos
# rdd_join = rdd_join.sortBy(lambda t: t[3], ascending=False)

# Agrupo por especie
# rdd_join = rdd_join.groupBy(lambda t: t[1])

# Extraigo los datos que me interesan
# resultados = rdd_join.map(lambda t: (t[0], [(f'mascota: {id_mascota}', f'cliente: {id_cliente}') for (
#     id_mascota, especie, id_cliente, votos) in list(t[1])][0:3])).collect()
