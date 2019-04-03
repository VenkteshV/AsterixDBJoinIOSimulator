import math
from Config import *

class Join:
    def __init__(self, config, id = 0):
        self.config = config
        self.numOfPartitions = config.numPartitions
        self.F = config.F
        self.build = None
        self.probe = None
        self.mem = config.memSize
        self.spilledStatus = [False] * config.numPartitions
        self.freeMem = self.mem
        self.id = id
        self.stats_ = None

    def stats(self):
        if self.stats_ is None:
            self.stats_ = self.build.stats() + self.probe.stats()
        return self.stats_

    def spilledPartitions(self):
        return sum(self.spilledStatus)

    def isSpilled(self, i):
        return self.spilledStatus[i]

    def spill(self, i):
        self.spilledStatus[i] = True

    def unspill(self, i): #after build is over, when we try to bring as many partition as possible, this method unspills those that get back in the memory.
        self.spilledStatus[i] = False

    def updateFreeMem(self):
        extra = self.spilledPartitions() # saves at least 1 frame for each spilled partition
        self.freeMem = self.mem - self.build.memoryUsed() - extra
        return self.freeMem

    def getRecursionDepth(self):
        if (len(self.probe.joins) == 0):
            return 0
        return max(j.getRecursionDepth() for j in self.probe.joins) + 1

    def run(self):
        self.build = Build(self.config.buildSize, self.mem, self)
        self.build.run()
        self.build.close()
        self.probe = Probe(self.config.probeSize, self)
        self.probe.init()
        self.probe.run()
        self.build.stats().recursionDepth = self.probe.stats().recursionDepth
        self.stats().recursionDepth = self.probe.stats().recursionDepth

    def __str__(self):
        return "\"Join\" : id %s, partitions: %s, numPartitions_spilled: %s, buildSize: %s, probeSize: %s, mem: %s, freeMem: %s" %(self.id,self.numOfPartitions, self.spilledPartitions() ,self.config.buildSize, self.config.probeSize, self.mem, self.freeMem)



class Build:
    def __init__(self, size, mem, join):
        self.partitions = []
        self.size = size
        self.mem = mem
        self.join = join
        self.stats_ = None

    def stats(self):
        if self.stats_ is None:
            self.stats_ = sum([p.stats() for p in self.partitions], Stats())
        return self.stats_

    def addPartition(self, p):
        self.partitions.append(p)

    def run(self):
        data_size = math.ceil(self.size / self.join.numOfPartitions)
        memForpartition = math.floor((self.mem - self.join.spilledPartitions())
                                      / (self.join.numOfPartitions - self.join.spilledPartitions()))
        totalSize = self.size
        for pindex in range(self.join.numOfPartitions):
           if totalSize == 0 :
               p = Partition(pindex, memForpartition, 0)
               self.addPartition(p)
               continue
           else:
                p = Partition(pindex, memForpartition, data_size)
                totalSize -= data_size
                # Reading of base relations should not be counted. Only intermediate results.
                #SeqR  occurs for the size of the memory given to this partition, after that we need to write to disk as memory is full which causes next reads to be separate seqR than current read.
                if self.join.id != 0:
                    remaining  = data_size
                    if remaining > memForpartition:
                        p.doSR(memForpartition)
                        remaining -= memForpartition
                        if remaining > 0:
                            p.doSR(remaining)
                        assert p.inMem == data_size

                #writes to disk
                if memForpartition < data_size :
                    self.join.spill(p.pid)
                    p.doSW(memForpartition)
                    if data_size - memForpartition > 0:
                        p.doRW(data_size - memForpartition)
                    if p.inMem != 0:
                        raise  Exception("p.InMem != 0 pid="+str(p.pid) +"inMem"+str(p.inMem)+str(self.join))
                self.addPartition(p)


    def memoryUsed(self):
       size = 0
       for i in range(len(self.join.spilledStatus)):
           if not self.join.spilledStatus[i]:
               size += self.partitions[i].size
       return size


    def close(self):
        # for now just bring back partitions to memory
        self.bringPartitionsBackinIfPossible()

    def bringPartitionsBackinIfPossible(self):
        freeMem = self.join.updateFreeMem()
        for p in self.partitions:
            if self.join.spilledStatus[p.pid] and p.inMem == 0 and p.size < freeMem:
                p.doSR(p.size)
                freeMem -= p.size
                self.join.unspill(p.pid)
            else:
                continue
        self.join.updateFreeMem()

class Probe:
    def __init__(self, size, parentJoin):
        self.partitions = []
        self.joins = []
        self.parentJoin = parentJoin
        self.size = size
        self.stats_ = None

    def stats(self):
        if self.stats_ is None:
            max_recursionDepth = 0 if len(self.joins) == 0 else max(j.stats().recursionDepth for j in self.joins) +1
            self.stats_ = (sum([p.stats() for p in self.partitions], Stats())
                        + sum([j.stats() for j in self.joins], Stats()))
            self.stats_.recursionDepth = max_recursionDepth
        return self.stats_

    def init(self):
        dataSizeForEachPartition = math.ceil(self.size / self.parentJoin.numOfPartitions)
        memForEachPartition = 1 if self.parentJoin.spilledPartitions() == 0 else 1+ math.floor(self.parentJoin.freeMem / self.parentJoin.spilledPartitions())
        totalSize = self.size
        for i in range(self.parentJoin.numOfPartitions):
            if totalSize == 0 or not self.parentJoin.isSpilled(i):
                self.partitions.append(Partition(i, 0, 0))
                continue
            else:
                totalSize -= dataSizeForEachPartition
                self.partitions.append(Partition(i, memForEachPartition,
                                                 dataSizeForEachPartition))
                partition = self.partitions[i]
                size = partition.size
                if size - partition.mem > 0:
                    self.partitions[i].doSW(partition.mem)
                    size -= partition.mem
                if size > 0:
                    self.partitions[i].doRW(size)
            # counting the seqR for reading intermediate results in
                # SeqR  occurs for the size of the memory given to this partition, after that we need to write to disk as memory is full which causes next reads to be separate seqR than current read.
                if self.parentJoin.id != 0:
                    remaining = dataSizeForEachPartition
                    while (remaining > partition.mem):
                        partition.doSR(partition.mem)
                        remaining -= partition.mem
                    if remaining > 0:
                        partition.doSR(remaining)




    def run(self):
        for i in range(self.parentJoin.numOfPartitions):
            if self.parentJoin.isSpilled(i):
                c = Config(self.parentJoin.build.partitions[i].size,
                           self.partitions[i].size,
                           self.parentJoin.mem,
                           self.parentJoin.F,
                           self.parentJoin.numOfPartitions)
                nextJoin = Join(c, self.parentJoin.id+1)
                self.joins.append(nextJoin)
                nextJoin.run()



    def __str__(self):
        return "\"Probe\" : partitions: %s, data_size: %s" %(self.partitions, self.size)


class Stats:
    seekTime_HDD = 12 #ms
    rotational_HDD = 4.17 #ms
    transferRate_HDD = 0.6 #MB/ms

    seqR_SSD = 3 # MB/ms
    seqW_SSD = 1.15 #MB/ms
    #randomR_SSD = 360 #iopms
    randomW_SSD = 280 #iopms


    def __init__(self, RW = 0, SW = 0, seqR = 0, seeks = 0, recursionDepth = 0, seqRSeeks=0, RWSeeks=0, SWSeeks=0):
        self.RW = RW
        self.SW = SW
        self.seqR = seqR
        self.seeks = seeks
        self.recursionDepth = recursionDepth
        self.seqRSeeks = seqRSeeks
        self.RWSeeks = RWSeeks
        self.SWSeeks = SWSeeks

    @property
    def totalIO(self):
        return self.RW + self.SW + self.seqR

    @property
    def totalTimeHDD(self):
        return self.seeks * (Stats.seekTime_HDD + 0.5 * Stats.rotational_HDD) + (self.totalIO / Stats.transferRate_HDD)

    @property
    def totalTimeSSD(self):
        return (self.seqR / Stats.seqR_SSD) + (self.SW / Stats.seqW_SSD) + (self.RW / Stats.randomW_SSD)

    @property
    def totalW(self):
        return self.RW + self.SW

    @staticmethod
    def getAttrNames():
        return ['RW', 'SW', 'seqR', 'totalIO', 'totalTimeHDD','totalTimeSSD', 'totalW', 'recursionDepth']


    def __add__(self, other):
        return Stats(self.RW + other.RW, self.SW + other.SW, self.seqR + other.seqR, self.seeks + other.seeks,
                     self.recursionDepth, self.seqRSeeks + other.seqRSeeks, self.RWSeeks + other.RWSeeks, self.SWSeeks + other.SWSeeks)

    def __str__(self):
        return (" SW(pages): %d\tRW: %d\tseqR: %d\tseeks: %d\ttotalTimeHDD(ms): %f \ttotalTimeHDD(ms): %f\ttotalIO: %d\ttotalW: %d\trecursionDepth: %d \tseqSeeks: %d \tRWSeeks: %d \tSWSeeks: %d"
                % (self.SW, self.RW, self.seqR, self.seeks, self.totalTimeHDD,self.totalTimeSSD, self.totalIO, self.totalW, self.recursionDepth, self.seqRSeeks, self.RWSeeks, self.SWSeeks))

class Partition:
    def __init__(self, pid, mem, size):
        self.pid = pid
        self.mem = mem
        self.size = size
        self.inMem = 0
        self.stats_ = Stats()

    def stats(self):
        return self.stats_

    def doRW(self, RW):  # randomwrite
        self.stats_.RW += int(RW)
        self.inMem = max(0,self.inMem - int(RW))
        self.stats_.seeks += int(RW)
        self.stats_.RWSeeks += int(RW)

    def doSW(self, SW):
        self.stats_.SW += int(SW)
        self.inMem = max( 0 ,self.inMem - int(SW))
        self.stats_.seeks += 1
        self.stats_.SWSeeks += 1

    def doSR(self,seqR): #for reading a whole partition in during build close
        self.stats_.seqR += int(seqR)
        self.inMem += seqR
        self.stats_.seeks += 1
        self.stats_.seqRSeeks += 1

    def __str__(self):
        return ("pid: " + str(self.pid) + " size: " + str(self.size) + " mem: " + str(self.mem)
                + " stats: \"" + str(self.stats_))
        