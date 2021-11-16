from MRE import Job


def fmap(key, value, context):
    context.write(value.lower(), 1)


def freduce(key, values, context):
    c = 0
    for v in values:
        c = c+1
    context.write(key, c)


inputDir = "./datasets/encuesta/"
outputDir = "./encuestaOut/"

job = Job(inputDir, outputDir, fmap, freduce)
success = job.waitForCompletion()
