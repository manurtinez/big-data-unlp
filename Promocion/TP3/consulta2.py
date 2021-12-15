from pyspark import SparkContext

sc = SparkContext('local', 'myapp')

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

# "Aplano" las tuplas luego del join
rdd_join = rdd_join.map(lambda t: (t[0], t[1][0], t[1][1][0], t[1][1][1]))

# Ordeno rdd por votos
rdd_join = rdd_join.sortBy(lambda t: t[3], ascending=False)

# Agrupo por especie
rdd_join = rdd_join.groupBy(lambda t: t[1])

# Extraigo los datos que me interesan
resultados = rdd_join.map(lambda t: (t[0], [(f'mascota: {id_mascota}', f'cliente: {id_cliente}') for (
    id_mascota, especie, id_cliente, votos) in list(t[1])][0:3])).collect()

# Imprimo resultados a consola
print(resultados)

# Guardo resultados a un archivo
with open('resultadosTopVotos.txt', 'w') as file:
    file.write('Tres pares mascota-cliente con mas votos para c/ especie\n')
    for tup_especie in resultados:
        file.write(f'{tup_especie[0]}:\n')
        for par in tup_especie[1]:
            file.write(str(par) + '\n')
    file.close()
