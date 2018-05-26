from BTS_infrastructure import *

# create_ts_file("right.txt", 0, 40, 5)
# create_ts_file("wrong.txt", 1, 40, 5)

BTS = BTS_infrastructure(1233, [i + 1234 for i in range(5)], 3, 5, 4, "right.txt", "wrong.txt")
#BTS = BTS_infrastructure(1233, [1234, 1235, 1236], 0, 3, 3, "right.txt", "wrong.txt")
BTS.run()