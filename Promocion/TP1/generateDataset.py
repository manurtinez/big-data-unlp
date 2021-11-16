import random

file = open('dataset.txt', 'w')
colours = ['negro', 'marron', 'blanco', 'gris']
for num in range(0,1000):
	tuple = '{}\t{}\t{}\t{}\t{}'.format(num, colours[random.randint(0,len(colours)-1)], random.randint(0,100), random.randint(0,22), random.randint(0,500))
	file.write(tuple + '\n')
file.close()