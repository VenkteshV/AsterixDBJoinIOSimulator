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
npartitions = 1
numOfJoins = 1
pageTransferTime = 0.2
seekTime = 0.5

#############Helper Functions#####################
def MBTOPages(MB):
    return int(MB*1024*1024/32768)

def pageToMB(pages):
    return int(pages*32768/1024/1024)

##################################################

class Stats:
    def __init__(self):
        pass



class Partition:
    def __init__(self,pid,mem,size):
        self.pid = pid
        self.mem = mem
        self.size = size
        self.inMem = 0
        self.RW = 0
        self.SW = 0
        self.seqR = 0 #during build: for brining paritions back at build close
        self.randR = 0
        self.seeks=0
    def doRW(self,RW):#randomwrite
        self.RW += int(RW)
        self.seeks +=int(RW)
    def doSW(self,SW):
        self.SW += int(SW)
        self.seeks +=1
    def doSR(self):
        self.seqR += self.size
        self.inMem = self.size
        self.seeks +=1
    def print(self):
        print("pid: "+ str(self.pid)+ " size: "+str(self.size)+" mem: "+str(self.mem) +" SW(pages): "+str(self.SW)+" RW: "+str(self.RW)+ " inMem: "+str(self.inMem))


class Join:

    def __init__(self,build_size,probe_size,mem,F,nPartitions,partitions):
        self.build_size = build_size
        self.probe_size = probe_size
        self.mem = mem
        self.F = F
        self.npartitions = nPartitions #cluster nodes a.k.a node partitions
        self.numOfPartitions = partitions #number of build parititions
        self.RpartitionObjs=[] #build partitions objs
        self.SpartitionObjs=[] #probe partitions objs
        self.spilledPartitions = 0
        self.freeMem = self.mem
        self.spilledStatus = [False] * partitions



    def DoBuild(self): #build happens here
        #data size
        data_size = int(self.build_size / self.numOfPartitions)
        for i in range(self.numOfPartitions):
            memForpartitionI = math.floor((self.mem -self.spilledPartitions) / (self.numOfPartitions - self.spilledPartitions))
            p = Partition(i, memForpartitionI, data_size)
            if memForpartitionI < data_size :
                self.spilledStatus[p.pid] = True
                p.doSW(memForpartitionI)
                if data_size - memForpartitionI > 0:
                    p.doRW(data_size-memForpartitionI)
                self.spilledPartitions += 1
            else:
                p.inMem = data_size
            self.RpartitionObjs.append(p)
        self.bringPartitionsBackinIfPossible()

    def calculateFreeMemory(self):
        FreeMem = self.mem
        for p in self.RpartitionObjs:
            FreeMem -= p.inMem
        return FreeMem - self.spilledPartitions #saves at least 1 frame for each spilled partition

    def bringPartitionsBackinIfPossible(self):
        self.freeMem = self.calculateFreeMemory()
        for p in self.RpartitionObjs:
            if p.inMem == 0 and p.size < self.freeMem:
                p.doSR()
                self.freeMem -= p.size
                self.spilledStatus[p.pid] = False
            else:
                break
        self.initProbe()

    def initProbe(self):
        for idx, p in enumerate(self.spilledStatus):
            if p == False:# corresponding build partition is in memory
                self.SpartitionObjs.append(Partition(idx, 0, 0))
            else:
                self.SpartitionObjs.append(Partition(idx, math.floor(self.freeMem / self.spilledPartitions), math.floor(self.probe_size / self.numOfPartitions)))
                partition =  self.SpartitionObjs[idx]
                size = partition.size
                while size - partition.mem > 0:
                    self.SpartitionObjs[idx].doSW(partition.mem)
                    size -= partition.mem
                if size > 0:
                    self.SpartitionObjs[idx].doRW(size)
                self.SpartitionObjs[idx].size = size
        self.joinPairPartitions()


    def joinPairPartitions(self):
        for idx,p in enumerate(self.spilledStatus):
            if p == False:
                continue
            else:
                nextJoin = Join(self.RpartitionObjs[idx].size, self.SpartitionObjs[idx].size, self.mem, self.F, self.npartitions, self.numOfPartitions)
                nextJoin.run()

    def run(self):
        self.DoBuild()

    def calculateBuildStats(self):
        global seekTime,pageTransferTime
        inMemPartitions = 0
        spilledPartitions = 0
        inDiskSize = 0
        inMemSize = 0
        RandomWMB = 0
        SeqWMB = 0
        SeqRMB = 0
        Seeks =0
        TotalTime = 0
        for part in self.RpartitionObjs:
            if part.inMem > 0:
                inMemPartitions += 1
                inMemSize += part.inMem
                SeqRMB += part.seqR

            else:
                spilledPartitions += 1
                inDiskSize += part.RW
                RandomWMB += part.RW
                inDiskSize += part.SW
                SeqWMB += part.SW
                Seeks += part.seeks
        TotalTime += (inDiskSize+SeqRMB)*pageTransferTime + Seeks*seekTime
        return self.build_size, inMemPartitions, spilledPartitions, inDiskSize, inMemSize, RandomWMB, SeqWMB, SeqRMB, TotalTime



    def printBuildStats(self):
        build,inMemPartitions, spilledPartitions, inDiskSize, inMemSize, RandomWMB, SeqWMB,SeqR, TotalTime = self.calculateBuildStats()
        print(str(pageToMB(build)) + "\t" + str(pageToMB(SeqWMB)) + "\t" + str(pageToMB(RandomWMB)) + "\t"+str(pageToMB(inDiskSize))+"\t"+str(TotalTime)+"\t"+str(pageToMB(SeqR)))




class Plot():
    def __init__(self,allJoins):
        self.allJoins = allJoins
    def buildPlotInfo(self):
        x = []
        seq = []
        rand = []
        totalW =[]
        totalT = []
        totalR=[]
        for j in self.allJoins:
            build, inMemPartitions, spilledPartitions, inDiskSize, inMemSize, RandomWMB, SeqWMB, SeqR, TotalTime = j.calculateBuildStats()
            x.append(int(pageToMB(build)))
            seq.append(int(pageToMB(SeqWMB)))
            rand.append(int(pageToMB(RandomWMB)))
            totalW.append(int(pageToMB(inDiskSize)))
            totalT.append(float(TotalTime))
            totalR.append(float(pageToMB(SeqR)))
        return [[x,seq,rand,totalW,totalT,totalR],["SeqW","RandW","TotalW","TotalTime","SeqR"]]


    def drawSeparate(self,xlabel,ylabel,title,xaxistext,xticks,yticks,sufix):
       if os.path.exists("./Partition" + sufix):
            shutil.rmtree("./Partition" + sufix)
       try:
            os.mkdir("./Partition" + sufix)
       except OSError:
           print("Creation of the directory %s failed" % sufix)
       stats = self.buildPlotInfo()
       #x,seq,rand,totalW,totalT,totalR
       for i in range(1,len(stats[1])):
           y = stats[0][i]
           lineName = stats[1][i-1]
           plt.clf()
           plt.step([1,2,3,4], y, label=lineName, marker="d",where='post')
           plt.ylim(bottom=0, top=1.5*max(y))
           plt.xlim(left=0)
           plt.xlabel(xlabel)
           plt.ylabel(ylabel)
           if xticks:
            plt.xticks([1,2,3,4],xaxistext)
           if yticks:
            plt.yticks(y,y)
           plt.grid()
           plt.title(title)
           plt.legend()
           plt.savefig("./Partition"+sufix+"/"+lineName+".png")



def buildSizeIsVar():
    global probe_size
    buildSize = [math.ceil(0.7 * mem), math.ceil(1.1 * mem),math.ceil(4 * mem),math.ceil(50*mem)]
    all=[]
    for partition in range(2,3):
        print("================================Partitions = "+str(partition)+" | Mem = "+str(mem)+" (MB) =========================================")
        print("builsSize, seqW, randW, TotalW, TotalTime, seqR")
        for bs in buildSize:
            join = Join(MBTOPages(bs), MBTOPages(bs), MBTOPages(mem)-2, F,npartitions, partition) #so far build_size and probe_size are set to be the same value change with probe_size if needed.
            join.run()
            join.printBuildStats()
            all.append(join)
        plot = Plot(all)
        plot.drawSeparate( "Build Size Class", "Writes(MB)",
               " Partitions = " + str(len(join.RpartitionObjs)) + " Memory = " + str(mem) + " (MB)", ["XS(0.7*Mem)", "S(1.1*Mem)", "M(4*Mem)", "L(50*Mem)"], True, False, str(partition))

buildSizeIsVar()