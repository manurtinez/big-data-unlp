from MRE import Job


def fmap(key, value, context):
    words = value.split()
    for w in words:
        context.write(w, 1)


def freduce(key, values, context):
    """
    Este reduce cuenta ocurrencias de cada palabra
    """
    c = 0
    for v in values:
        c = c+1
    context.write(key, c)


def fmap2(key, value, context):
    # word, count = value.split('\t')
    context.write(1, (key, value))


def freduce2(key, values, context):
    """
    Este reduce saca maximo y minimo de ocurrencias
    """
    # key: 1  values: (word, count)
    max = -1
    min = 99999
    max_word = ''
    min_word = ''
    for v in values:
        if int(v[1]) > max:
            max = int(v[1])
            max_word = v[0]
        if int(v[1]) < min:
            min = int(v[1])
            min_word = v[0]
    context.write('max / min', f'{max_word} / {min_word}')


def freduce3(key, values, context):
    """
    Este reduce saca el promedio de ocurrencias
    """
    total = 0
    count = 0
    for v in values:
        count += 1
        total += int(v[1])
    context.write('promedio', total / count)


inputDir = "../datasets/libros/"
outputDir = "./wordCountOut/"
inputDir2 = "./wordCountOut/"
outputDir2 = "./wordCountMaxMinOut/"
outputDir3 = "./wordCountMeanOut/"

job = Job(inputDir, outputDir, fmap, freduce)
success = job.waitForCompletion()

job2 = Job(inputDir2, outputDir2, fmap2, freduce2)
success = job2.waitForCompletion()

job3 = Job(inputDir2, outputDir3, fmap2, freduce3)
success = job3.waitForCompletion()
