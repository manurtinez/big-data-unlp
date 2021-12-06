datos = [('true true    false   true'), ('false true    true   true'),
         ('true false    false   false')]

m = {}

for t in datos:
    v = t.split("\t")
    c = v[-1]
    for a in range(len(v)-1):
        x = v[a]
        m[a][x][c] = m[a][x][c] + 1

max = [[0, 0, 0], [0, 0, 0]]

for x in range(len(m)):
    for y in range(len(m[0])):
        for z in range(2):
            if(m[x][y][z] > max[z][0]):
                max[z][0] = m[x][y][z]
                max[z][1] = x
                max[z][2] = y

for z in range(2):
    print(str(z)+";"+str(max[z][1])+";"+str(max[z][2]))
