from ExperimentAggFunctions import *
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt

class Plot:
    def __init__(self, **kwargs):
        self.runs = kwargs['runs']
        self.phase = kwargs['phase']
        self.outFolder = kwargs['outFolder']
        self.xlabel = kwargs['xlabel']
        # self.numPartitions = kwargs['numPartitions']
        self.metric_fn = kwargs['metric_fn']
        self.groupBy_fn = kwargs['groupBy_fn']
        self.select_fn = kwargs['select_fn']
        # self.xticks = self.x
        # if 'xticks' in kwargs:
        #     self.xticks = kwargs['xticks']

    def plot(self):
        groups = ExperimentAggFunctions.groupBy(self.runs, self.groupBy_fn)
        S = ExperimentAggFunctions.select(groups, self.select_fn)

        for g, runs in groups.items():
            ys = []
            for r in runs:
                if self.phase == 'build':
                    s = r.join.build.stats()
                elif self.phase == 'probe':
                    s = r.join.probe.stats()
                else:
                    s = r.join.stats()
                ys.append(self.metric_fn(s))

            plt.step(S[g], ys)
        plt.savefig('test.png')
