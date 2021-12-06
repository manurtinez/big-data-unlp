import random

datos = [(2, 5, 2, 1), (1, 9, 2, 6), (1, 6, 7, 4)]

error = 0.001
dif = 1
K = 4
M = 4

prom = [0, 0, 0, 0, 0]
N = len(datos)

for t in datos:
    for m in range(M):
        prom[m] = prom[m] + float(t[m])

C = []

for m in range(K):
    e = []
    for m in range(M):
        e.append(prom[m] / N + random.random())
    C.append(e)

while dif > error:
    S = []
    print(dif)
    for m in range(K):
        S.append([[0, 0, 0, 0, 0], 0])

    for t in datos:
        min = 99999
        for q in range(K):
            a = 0
            for m in range(M):
                a = a + (C[q][m] - float(t[m])) ** 2
            if (a < min):
                min = a
                Q = q
        for m in range(M):
            S[Q][0][m] = S[Q][0][m] + float(t[m])
        S[Q][1] = S[Q][1] + 1
    for q in range(K):
        if S[q][1] > 0:
            for m in range(M):
                S[q][0][m] = S[q][0][m] / S[q][1]

    dif = 0
    for q in range(K):
        for m in range(M):
            dif = dif+(S[q][0][m] - C[q][m]) ** 2
            if S[q][1] > 0:
                C[q][m] = S[q][0][m]
