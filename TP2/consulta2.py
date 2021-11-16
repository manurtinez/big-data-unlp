from MRE import Job


def fmap(key, value, context):
    values = value.split('\t')
    if len(values) == 3:  # Es el dataset de solicitudes
        context.write((key, 'sol'), values)
    else:   # Es el de mascotas
        context.write((key, 'mas'), values)


def freduce(key, values, context):
    mascota = values.next()
    for v in values:
        usuario = v
        context.write(key[0], mascota + usuario)


def fmap2(key, value, context):
    # Suponiendo una tupla del join como:
    # 1	perro	raza3	negro	26	2017-11-15	1102	Rcltvmtj	20
    # id_mascota    especie raza    color   edad    fecha   id_usuario  motivo  votos
    values = value.split('\t')
    id_usuario = values[5]
    votos = values[7]
    especie = values[0]
    context.write(especie, (key, id_usuario, votos))


def freduce2(key, values, context):
    # Suponiendo key = especie y values:
    # (id_mascota    id_usuario  votos)
    max = [0, 0, 0]
    tuplas_max = [(), (), ()]
    for v in values:
        if int(v[2]) > max[0]:
            # Se mueven hacia abajo los otros 2 puestos
            max[2] = max[1]
            max[1] = max[0]
            max[0] = int(v[2])
            # Se actualiza el valor del primer puesto
            tuplas_max[0] = (v[0], v[1])
        elif int(v[2]) > max[1]:
            # Se mueve hacia abajo el 2do puesto
            max[2] = max[1]
            max[1] = int(v[2])
            # Se actualiza valor del 2do puesto
            tuplas_max[1] = (v[0], v[1])
        elif int(v[2]) > max[2]:
            max[2] = int(v[2])
            # Actualiza valor de 3er puesto
            tuplas_max[2] = (v[0], v[1])
    context.write(key, tuplas_max)


def cmpSort(key1, key2):
    if(key1[1] == key2[1]):
        return 0
    elif(key1[1] == 'mas'):
        return -1
    else:
        return 1


def cmpShuffle(key1, key2):
    if(key1[0] == key2[0]):
        return 0
    elif(key1[0] < key2[0]):
        return -1
    else:
        return 1


inputDir = "./datasets/"
outputDir = "./joinMascotasUsuario/"
inputDir2 = "./joinMascotasUsuario/"
outputDir2 = "./topVotos/"

job = Job(inputDir, outputDir, fmap, freduce)
job.setShuffleCmp(cmpShuffle)
job.setSortCmp(cmpSort)
success = job.waitForCompletion()

job2 = Job(inputDir2, outputDir2, fmap2, freduce2)
success = job2.waitForCompletion()
