from MRE import Job


def fmap(key, value, context):
    words = value.split()
    for w in words:
        context.write(w, 1)


def freduce(key, values, context):
    c = 0
    for v in values:
        c = c+1
    context.write(key, c)


def fcombiner(key, values, context):
    c = 0
    for v in values:
        c += 1
    context.write(key, c)


inputDir = "../Practica1/datasets/libros/"
outputDir = "./wordCountOut/"

job = Job(inputDir, outputDir, fmap, freduce)
job.setCombiner(fcombiner)
success = job.waitForCompletion()
