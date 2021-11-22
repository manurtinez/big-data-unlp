from MRE import Job
import re

# genero regex de comparacion
vocales = re.compile('[aeiou]', re.IGNORECASE)
consonantes = re.compile('(?![aeiou])[a-z]', re.IGNORECASE)
digitos = re.compile('[0-9]')
specialChars = re.compile('(?! )\W|_')


def fmap(key, value, context):
    words = value.split()
    for w in words:
        for char in w:
            if vocales.match(char):
                context.write('vocales', 1)
            elif consonantes.match(char):
                context.write('consonantes', 1)
            elif digitos.match(char):
                context.write('digitos', 1)
            elif specialChars.match(char):
                context.write('especiales', 1)
            else:
                context.write('espacios', 1)


def freduce(key, values, context):
    c = 0
    for v in values:
        c = c+1
    context.write(key, c)


inputDir = "../datasets/libros/"
outputDir = "./wordCountCategoriesOut/"

job = Job(inputDir, outputDir, fmap, freduce)
success = job.waitForCompletion()
