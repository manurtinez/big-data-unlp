from sys import argv
from MRE import Job

# Se recibe la especie por parametro
# Por ejemplo, python consultas.py "perro"
try:
    especie_param = argv[1]
except IndexError:  # No se paso el parametro necesario
    print('Debe pasarse por argumento la especie deseada')
    exit()


def fmap(key, value, context):
    values = value.split('\t')  # especie, raza, color, edad, fecha
    if len(values) == 5:  # ignorar dataset solicitudes
        especie = values[0]
        raza = values[1]
        color = values[2]
        # Ignorar todas las otras especies
        if especie == especie_param:
            context.write((raza, color), 1)


def freduce(key, values, context):
    count = 0
    for v in values:
        count += 1
    context.write(key, count)


def fmap2(key, value, context):
    raza = key
    color, acum = value.split('\t')
    context.write(raza, (color, acum))


def freduce2(key, values, context):
    max = -1
    color_max = ''
    for v in values:
        if int(v[1]) > max:
            max = int(v[1])
            color_max = v[0]
    context.write(key, color_max)


inputDir = "./datasets/"
outputDir = "./colorRazaAcumulados/"
inputDir2 = "./colorRazaAcumulados/"
outputDir2 = "./maximoColorPorRaza/"

job = Job(inputDir, outputDir, fmap, freduce)
success = job.waitForCompletion()

job2 = Job(inputDir2, outputDir2, fmap2, freduce2)
success = job2.waitForCompletion()
