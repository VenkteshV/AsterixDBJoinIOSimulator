from Join import *
from Config import *


class Experiment:
    def __init__(self, buildSizes, probeSizes, memSizes, F, numPartitions):
        self.buildSizes = buildSizes
        self.probeSizes = probeSizes
        self.memSizes = memSizes
        self.F = F
        self.numPartitions = numPartitions
        self.done = False
        self.runs = []

    def generateConfigs(self):
        assert len(self.buildSizes) == len(self.probeSizes)
        for bs, ps in zip(self.buildSizes, self.probeSizes):
            for m in self.memSizes:
                for p in self.numPartitions:
                    yield Config(MBToFrames(bs), MBToFrames(ps), MBToFrames(m), self.F, p)

    def run(self):
        assert not self.done
        for c in self.generateConfigs():
            r = Run(c)
            self.runs.append(r)
            r.run()
        self.done = True


class Run:
    def __init__(self,config):
        self.config = config
        self.join = None

    def run(self):
        self.join = Join(self.config)
        self.join.run()