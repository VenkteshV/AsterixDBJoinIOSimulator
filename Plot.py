from ExperimentAggFunctions import *
import os
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt

class Plot:
    def __init__(self, **kwargs):
        self.runs = kwargs['runs']
        self.phase = kwargs['phase']
        self.outFolder = kwargs['outFolder']
        self.outFile = kwargs['outFile']
        self.xlabel = kwargs['xlabel']
        self.ylabel = kwargs['ylabel']
        self.metric_fn = kwargs['metric_fn']
        self.groupBy_fn = kwargs['groupBy_fn']
        self.select_fn = kwargs['select_fn']
        self.lineLabel_fn = kwargs['lineLabel_fn']
        self.xlog = kwargs['xlog']
        self.ylog = kwargs['ylog']

    def fixYvalues(self, ys):
        return [1 + y for y in ys]

    def plot(self):
        # Create folders
        try:
            os.makedirs(self.outFolder, exist_ok = True)
        except OSError:
            print('Error creating output folder %s' % (self.outFolder))

        groups = ExperimentAggFunctions.groupBy(self.runs, self.groupBy_fn)
        S = ExperimentAggFunctions.select(groups, self.select_fn)

        plt.figure(figsize = (12, 10))
        if self.ylog:
            plt.yscale('log')
            self.ylabel += "(log)"
        if self.xlog:
            plt.xscale('log')
            self.xlabel += "log"
        plt.grid()
        allYs =[]
        xticks=[]
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
            if self.ylog:
                ys = self.fixYvalues(ys)
            allYs += ys
            xticks = [0]+S[g]
            #plt.step(S[g], self.fixYvalues(ys), label = self.lineLabel_fn(g), marker = 'x', where="post")
            plt.plot(S[g], ys, label=self.lineLabel_fn(g),alpha=0.6)

        plt.ylabel(self.ylabel)
        plt.xlabel(self.xlabel)
        #plt.yticks(allYs, allYs)
        plt.xlim(0)
        #plt.xticks(xticks,xticks)
        plt.legend()
        plt.savefig(self.outFolder + '/' + self.outFile)
        plt.close()
