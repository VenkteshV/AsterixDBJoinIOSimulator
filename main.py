from enum import Enum
import math
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import os
import shutil
build_size = 1005
probe_size = 1005
F=1.3
mem = 256
numOfJoins = 1
pageTransferTime = 0.2
seekTime = 0.5

#############Helper Functions#####################
def MBTOPages(MB):
    return int(MB*1024*1024/32768)

def pageToMB(pages):
    return int(pages*32768/1024/1024)

##################################################


class Join:
    def __init__(self, build_size, probe_size, mem, F, numOfPartitions):
        self.numOfPartitions = numOfPartitions
        self.F = F
        self.build = Build(build_size, mem, self)
        self.probe = Probe(probe_size, self)
        self.mem = mem
        self.spilledStatus = [False] * numOfPartitions
        self.freeMem = self.mem

    def stats(self):
        return self.build.stats() + self.probe.stats()

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

    def run(self):
        self.build.run()
        self.build.close()
        self.probe.init()
        self.probe.run()

class Build:
    def __init__(self, size, mem, join):
        self.partitions = []
        self.size = size
        self.mem = mem
        self.join = join

    def stats(self):
        return sum([p.stats() for p in self.partitions], Stats())

    def addPartition(self, p):
        self.partitions.append(p)

    def run(self):
        data_size = int(self.size / self.join.numOfPartitions)
        for i in range(self.join.numOfPartitions):
            memForpartitionI = math.floor((self.mem - self.join.spilledPartitions())
                                          / (self.join.numOfPartitions - self.join.spilledPartitions()))
            p = Partition(i, memForpartitionI, data_size)
            if memForpartitionI < data_size :
                self.join.spill(p.pid)
                p.doSW(memForpartitionI)
                if data_size - memForpartitionI > 0:
                    p.doRW(data_size - memForpartitionI)
            else:
                p.inMem = data_size
            self.addPartition(p)

    def memoryUsed(self):
        return sum([p.inMem for p in self.partitions])

    def close(self):
        # for now just bring back partitions to memory
        self.bringPartitionsBackinIfPossible()

    def bringPartitionsBackinIfPossible(self):
        freeMem = self.join.updateFreeMem()
        for p in self.partitions:
            if p.inMem == 0 and p.size < freeMem:
                p.doSR()
                freeMem -= p.size
                self.join.unspill(p.pid)
            else:
                break
        self.join.updateFreeMem()

class Probe:
    def __init__(self, size, parentJoin):
        self.partitions = []
        self.joins = []
        self.parentJoin = parentJoin
        self.size = size

    def stats(self):
        return (sum([p.stats() for p in self.partitions], Stats())
              + sum([j.stats() for j in self.joins], Stats()))

    def init(self):
        for i in range(self.parentJoin.numOfPartitions):
            if not self.parentJoin.isSpilled(i):
                self.partitions.append(Partition(i, 0, 0))
            else:
                self.partitions.append(Partition(i, math.floor(self.parentJoin.freeMem / self.parentJoin.spilledPartitions()),
                                                    math.floor(self.size / self.parentJoin.numOfPartitions)))
                partition = self.partitions[i]
                size = partition.size
                while size - partition.mem > 0:
                    self.partitions[i].doSW(partition.mem)
                    size -= partition.mem
                if size > 0:
                    self.partitions[i].doRW(size)
                self.partitions[i].size = size

    def run(self):
        for i in range(self.parentJoin.numOfPartitions):
            if self.parentJoin.isSpilled(i):
                nextJoin = Join(self.parentJoin.build.partitions[i].size,
                                self.partitions[i].size,
                                self.parentJoin.mem,
                                self.parentJoin.F,
                                self.parentJoin.numOfPartitions)
                self.joins.append(nextJoin)
                nextJoin.run()


class Stats:
    def __init__(self, RW = 0, SW = 0, seqR = 0, seeks = 0):
        self.RW = RW
        self.SW = SW
        self.seqR = seqR
        self.seeks = seeks

    def __add__(self, other):
        return Stats(self.RW + other.RW, self.SW + other.SW, self.seqR + other.seqR, self.seeks + other.seeks)

    def __str__(self):
        return " SW(pages): " + str(self.SW) + " RW: " + str(self.RW) + " seqR: " + str(self.seqR) + " seeks: " + str(self.seeks)

class Partition:
    def __init__(self, pid, mem, size, stats = Stats()):
        self.pid = pid
        self.mem = mem
        self.size = size
        self.inMem = 0
        self.stats_ = stats

    def stats(self):
        return self.stats_

    def doRW(self, RW):  # randomwrite
        self.stats_.RW += int(RW)
        self.stats_.seeks += int(RW)

    def doSW(self, SW):
        self.stats_.SW += int(SW)
        self.stats_.seeks += 1

    def doSR(self):
        self.stats_.seqR += self.size
        self.stats_.inMem = self.size
        self.stats_.seeks += 1

    def __str__(self):
        return ("pid: " + str(self.pid) + " size: " + str(self.size) + " mem: " + str(self.mem) + " SW(pages): "
                + "stats: \"" + str(self.stats_) + "\" inMem: " + str(self.inMem))


# class Plot():
#     def __init__(self,allJoins):
#         self.allJoins = allJoins
#     def buildPlotInfo(self):
#         x = []
#         seq = []
#         rand = []
#         totalW =[]
#         totalT = []
#         totalR=[]
#         for j in self.allJoins:
#             build, inMemPartitions, spilledPartitions, inDiskSize, inMemSize, RandomWMB, SeqWMB, SeqR, TotalTime = j.calculateBuildStats()
#             x.append(int(pageToMB(build)))
#             seq.append(int(pageToMB(SeqWMB)))
#             rand.append(int(pageToMB(RandomWMB)))
#             totalW.append(int(pageToMB(inDiskSize)))
#             totalT.append(float(TotalTime))
#             totalR.append(float(pageToMB(SeqR)))
#         return [[x,seq,rand,totalW,totalT,totalR],["SeqW","RandW","TotalW","TotalTime","SeqR"]]
#
#
#     def drawSeparate(self,xlabel,ylabel,title,xaxistext,xticks,yticks,sufix):
#        if os.path.exists("./Partition" + sufix):
#             shutil.rmtree("./Partition" + sufix)
#        try:
#             os.mkdir("./Partition" + sufix)
#        except OSError:
#            print("Creation of the directory %s failed" % sufix)
#        stats = self.buildPlotInfo()
#        #x,seq,rand,totalW,totalT,totalR
#        for i in range(1,len(stats[1])):
#            y = stats[0][i]
#            lineName = stats[1][i-1]
#            plt.clf()
#            plt.step([1,2,3,4], y, label=lineName, marker="d",where='post')
#            plt.ylim(bottom=0, top=1.5*max(y))
#            plt.xlim(left=0)
#            plt.xlabel(xlabel)
#            plt.ylabel(ylabel)
#            if xticks:
#             plt.xticks([1,2,3,4],xaxistext)
#            if yticks:
#             plt.yticks(y,y)
#            plt.grid()
#            plt.title(title)
#            plt.legend()
#            plt.savefig("./Partition"+sufix+"/"+lineName+".png")
#
#
#
# def buildSizeIsVar():
#     global probe_size
#     buildSize = [math.ceil(0.7 * mem), math.ceil(1.1 * mem),math.ceil(4 * mem),math.ceil(50*mem)]
#     all=[]
#     for partition in range(2,3):
#         print("================================Partitions = "+str(partition)+" | Mem = "+str(mem)+" (MB) =========================================")
#         print("builsSize, seqW, randW, TotalW, TotalTime, seqR")
#         for bs in buildSize:
#             join = Join(MBTOPages(bs), MBTOPages(bs), MBTOPages(mem)-2, F,npartitions, partition) #so far build_size and probe_size are set to be the same value change with probe_size if needed.
#             join.run()
#             join.printBuildStats()
#             all.append(join)
#         plot = Plot(all)
#         plot.drawSeparate( "Build Size Class", "Writes(MB)",
#                " Partitions = " + str(len(join.RpartitionObjs)) + " Memory = " + str(mem) + " (MB)", ["XS(0.7*Mem)", "S(1.1*Mem)", "M(4*Mem)", "L(50*Mem)"], True, False, str(partition))
#
# buildSizeIsVar()