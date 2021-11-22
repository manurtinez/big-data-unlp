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


inputDir = "../datasets/libros/"
outputDir = "./wordCountOut/"

job = Job(inputDir, outputDir, fmap, freduce)
success = job.waitForCompletion()
