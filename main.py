import math
from Join import *

build_size = math.ceil(1.5 * 256)
probe_size = 0
mem = 256
F = 1.3
partitions = 2

J = Join(build_size, probe_size, mem, F, partitions)
J.run()
print(J.stats())