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
        assert self.numOfPartitions <= self.mem

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
        totalMem = self.mem
        totalSize = self.size
        id = 0
        for i in range(self.join.numOfPartitions):
            self.addPartition(Partition(i, 0))
            # Reading of base relations should not be counted. Only intermediate results.
            # SeqR  occurs for the size of the memory given to this partition, after that we need to write to disk as memory is full which causes next reads to be separate seqR than current read.
        if self.join.id != 0:
            self.partitions[0].doSR(totalSize)
        while(totalSize > 0):
            pindex = id % self.join.numOfPartitions
            if totalMem > 0 and not self.join.spilledStatus[pindex]:
                self.partitions[pindex].doInsert(1)
                totalMem -=1
                totalSize -=1
            else:
                #find victim
                if self.partitions[pindex].size == 0:
                    #spill biggest non-spilled partition
                    max = 0
                    pidMax = -1
                    for p in self.partitions:
                        if not self.join.spilledStatus(p.pid) and p.inMem > max:
                            pidMax = p.pid
                    assert pidMax >= 0
                    p = self.partitions[pidMax]
                else:
                    p = self.partitions[pindex]
                #spill or write
                if self.join.spilledStatus[p.pid]:
                    assert p.inMem == 1
                    p.doRW(p.inMem)
                    assert p.inMem == 0
                    assert p.mem == 0
                else:
                    freed_mem =p.inMem - 1
                    totalMem += freed_mem
                    p.doSW(p.inMem)
                    assert p.inMem == 0
                    assert p.mem == 0
                    self.join.spill(p.pid)
                p.doInsert(1)
                assert p.inMem == 1
                assert p.mem == 1
                totalSize -=1
            id += 1


    def memoryUsed(self):
       size = 0
       for i in range(len(self.join.spilledStatus)):
           if not self.join.spilledStatus[i]:
               size += self.partitions[i].inMem
       return size


    def close(self):
       #write the leftover parts of each spilled partition to the disk
        for p in self.partitions:
            if self.join.spilledStatus[p.pid]:
                assert p.inMem <=1
                if p.inMem == 1:
                    p.doRW(p.inMem)
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
        totalSize = self.size
        id = 0

        for i in range(self.parentJoin.numOfPartitions):
            self.partitions.append(Partition(i, 0))

        # Reading of base relations should not be counted. Only intermediate results.
        # SeqR  occurs for the size of the memory given to this partition, after that we need to write to disk as memory is full which causes next reads to be separate seqR than current read.
        if self.parentJoin.id != 0:
            self.partitions[0].doSR(totalSize)

        while (totalSize > 0):
            pindex = id % self.parentJoin.numOfPartitions
            id += 1
            totalSize -= 1
            p = self.partitions[pindex]
            #corresponding buildpartition is in memory
            if not self.parentJoin.isSpilled(pindex):
                p.size += 1
                continue
            else:
                #build partition is spilled
                p.doInsert(1)
                p.doRW(1)





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
    transferRate_HDD = 1.92 # Frames/ms or 0.06MB/ms

    seqR_SSD = 96 #Frames/ms or  3 MB/ms
    seqW_SSD = 36.8 # Frames/ms or 1.15 MB/ms
    randomR_SSD = 360 #iopms
    randomW_SSD = 35 #iopms(frame-based) or 280 iopms (4k-based) ==> calculation (280*4*1024)/32768


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
        return (" SW(pages): %d\tRW: %d\tseqR: %d\tseeks: %d\ttotalTimeHDD(ms): %f \ttotalTimeSSD(ms): %f\ttotalIO: %d\ttotalW: %d\trecursionDepth: %d \tseqSeeks: %d \tRWSeeks: %d \tSWSeeks: %d"
                % (self.SW, self.RW, self.seqR, self.seeks, self.totalTimeHDD,self.totalTimeSSD, self.totalIO, self.totalW, self.recursionDepth, self.seqRSeeks, self.RWSeeks, self.SWSeeks))

class Partition:
    def __init__(self, pid, size):
        self.pid = pid
        self.size = size
        self.inMem = 0
        self.mem = 0
        self.stats_ = Stats()

    def stats(self):
        return self.stats_

    def doRW(self, RW):  # randomwrite
        self.stats_.RW += int(RW)
        self.inMem = max(0,self.inMem - int(RW))
        self.mem = max(0, self.mem - int(RW))
        self.stats_.seeks += int(RW)
        self.stats_.RWSeeks += int(RW)

    def doSW(self, SW):
        self.stats_.SW += int(SW)
        self.inMem = max( 0 ,self.inMem - int(SW))
        self.mem = max(0, self.mem - int(SW))
        self.stats_.seeks += 1
        self.stats_.SWSeeks += 1

    def doSR(self,seqR): #for reading a whole partition in during build close
        self.stats_.seqR += int(seqR)
        self.stats_.seeks += 1
        self.stats_.seqRSeeks += 1

    def doInsert(self,R):
        self.mem += R
        self.inMem += R
        self.size += R

    def __str__(self):
        return ("pid: " + str(self.pid) + " size: " + str(self.size) + " inMem: "+ str(self.inMem)
                + " stats: \"" + str(self.stats_))
        