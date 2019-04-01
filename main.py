import sys
import json
from Join import *
from Experiment import *
from ExperimentAggFunctions import *
from Plot import *

if len(sys.argv) != 2:
    print('Usage: %s <config_file>' % sys.argv[0])
    sys.exit(1)

configFile = sys.argv[1]

with open(configFile) as json_file:  
    # Load data
    c = json.load(json_file)
    buildSizes = c['buildSizes']
    probeSizes = c['probeSizes']
    memSizes = c['memSizes']
    F = c['F']
    numPartitions = c['numPartitions']

    # Run Experiments
    E = Experiment(buildSizes, probeSizes, memSizes, F, numPartitions)
    E.run()
    for r in E.runs:
        print(r.config, r.join.stats())

    # Do plots
    for m in Stats.getAttrNames():
        for ph in ['build', 'probe', 'total']:
            P = Plot(runs = E.runs,
                     phase = ph,
                     outFolder = 'plots_' + str(numPartitions) + '/' + ph,
                     outFile = ph + '_' + m + '.png',
                     ylabel = 'bla',
                     xlabel = 'Xs',
                     metric_fn = lambda s: getattr(s, m),
                     groupBy_fn = lambda c: (c.buildSize, c.memSize),
                     select_fn = lambda c: c.numPartitions,
                     lineLabel_fn = lambda g: "(buildSize, memSize) = %s" % (str(g)))
            P.plot()
