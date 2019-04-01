import matplotliv

class Plot:
	def __init__(self, **kargs):
		self.runs = kwargs['runs']
		self.label = kwargs['label']
		self.outFolder = kwargs['outFolder']
		self.x = kwargs['x']
		self.numPartitions = kwargs['numPartitions']
		self.metric = kwargs['metric']
		self.phase = kwargs['phase']
		self.xticks = self.x
		if 'xticks' in kwargs:
			self.xticks = kwargs['xticks']

	def plot(self):
		yValues = []
		for r in self.runs:
			if self.phase == 'build':
				s = r.join.build.stats()
			elif self.phase == 'probe':
				s = r.join.probe.stats()
			else:
				s = r.join.stats()
			yValues.append(getattr(s, self.metric))





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
