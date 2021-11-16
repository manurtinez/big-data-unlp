from MRE import Job


def map(key, value, context):
    values = value.split('\t')
    for v in value:
        context.write(v, 1)


def reduce(key, values, context):
    c = 0
    for v in values:
        c = c+1
    context.write(key, c)


inputDir = "./datasets/"
outputDir = "./outputDir/"

job = Job(inputDir, outputDir, map, reduce)

success = job.waitForCompletion()

# abrir archivo resultante de 
file = open(outputDir + 'output.txt', 'r')
lines = file.readlines()

# generar una lista de los resultados
ranking = []
for line in lines:
    tuple = line.split('\t')
    # print(tuple[1])
    tuple[1] = int(tuple[1])
    ranking.append(tuple)

# ordenar la lista e imprimir el top 10, opcionalmente dejandola en un nuevo archivo
ranking.sort(key=lambda pair: pair[1], reverse=True)
top10 = ranking[:10]
print(top10)

# escribir archivo de salida
outputFile = open(outputDir2 + 'output.txt', 'w')
outputFile.write("Top 10 de palabras: \n")
for elem in top10:
    outputFile.write(elem[0] + ", " + str(elem[1]) + '\n')
outputFile.close()
