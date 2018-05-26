from client import *
from concurrent.futures import ThreadPoolExecutor

C = Client(1233)

"""
for i in range(0, 40):
    t = tuple(j for j in range(i, 5 + i))
    tt = C.out_op(t)
    print(tt)
"""

with ThreadPoolExecutor(10) as pool:
    i = 0
    while i < 40:
        t = tuple(j for j in range(i, 5 + i))
        i += 1
        print(i)
        pool.submit(C.in_op, t)
        #pool.submit(C.rd_op, t)
        #pool.submit(C.out_op, t)


# print(C.in_op(tuple(j for j in range(0, 5))))
C.stop_op()
