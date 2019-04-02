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
    rangeOverNumPartitions = c['rangeOverNumPartitions']
    folderName = c['folderName']

    groupBy_fn_ = c['groupBy_fn']
    select_fn_ = c['select_fn']
    lineLabel_fn_ = c['lineLabel_fn']
    xLabel = c['xLabel']
    xlog = c['xlog']
    ylog = c['ylog']

    # Run Experiments
    if rangeOverNumPartitions:
        assert len(numPartitions) == 2
        start = numPartitions[0]
        end = numPartitions[1]
        numPartitions = [i for i in range(start, end+1)]
    E = Experiment(buildSizes, probeSizes, memSizes, F, numPartitions)
    E.run()
    for r in E.runs:
        print(r.join, r.join.stats())
        print ("recursion: ", r.join.getRecursionDepth())
        print("build: ", r.join.build.stats())
        print("probe: ", r.join.probe.stats())


    # Do plots
    buildDone = False
    for ph in ['build', 'probe', 'total']:
        for m in Stats.getAttrNames():
            P = Plot(runs = E.runs,
                     phase = ph,
                     outFolder = folderName+ '/' + ph,
                     outFile = ph + '_' + m + '.png',
                     ylabel =  "Time (ms)" if (m=="totalTimeSSD" or m=="totalTimeHDD") else "I/O (MB)",
                     xlabel = xLabel,
                     xlog = xlog,
                     ylog  = ylog,
                     metric_fn = lambda s: getattr(s, m),
                     groupBy_fn = eval(groupBy_fn_) ,
                     select_fn = eval(select_fn_) ,
                     lineLabel_fn = eval(lineLabel_fn_))
            P.plot()
