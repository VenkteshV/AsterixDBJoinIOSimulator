class Config:
    def __init__(self, buildSize, probeSize, memSize, F, numPartitions):
        self.buildSize = buildSize
        self.probeSize = probeSize
        self.memSize = memSize
        self.F = F
        self.numPartitions = numPartitions

    def __str__(self):
        return "build size : %s , probe size: %s , mem: %s ,  partitions: %s" %(self.buildSize,  self.probeSize , self.memSize , self.numPartitions)