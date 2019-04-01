import math
from Join import *
from Experiment import *
from ExperimentAggFunctions import *
from Plot import *

buildSizes = [128, 256]
probeSizes = [128, 256]
memSizes = [255, 256]
F = 1.3
numPartitions = [2, 3, 4]

E = Experiment(buildSizes, probeSizes, memSizes, F, numPartitions)
E.run()

for r in E.runs:
    print(r.config, r.join.stats())

for m in Stats.getAttrNames():
    for ph in ['build', 'probe', 'total']:
        P = Plot(runs = E.runs,
                 phase = ph,
                 outFolder = 'plots',
                 xlabel = 'Xs',
                 metric_fn = lambda s: getattr(s, m),
                 groupBy_fn = lambda c: (c.buildSize, c.memSize),
                 select_fn = lambda c: c.numPartitions)
        P.plot()
