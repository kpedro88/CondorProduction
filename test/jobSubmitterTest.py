from Condor.Production.jobSubmitter import *

class jobSubmitterTest(jobSubmitter):
    def __init__(self):
        super(jobSubmitterTest,self).__init__()
        self.scripts = ["test.sh"]

    def addExtraOptions(self,parser):
        super(jobSubmitterTest,self).addExtraOptions(parser)
        parser.add_option("-N", "--nParts", dest="nParts", default=1, type="int", help="number of parts to process (default = %default)")
        parser.add_option("--name", dest="name", default="test", help="job name (default = %default)")
        parser.add_option("--io", dest="io", default=False, action="store_true", help="use I/O options (default = %default)")
        parser.add_option("--indir", dest="indir", default="", help="input file directory (local or PFN) (default = %default)")
        parser.add_option("--outdir", dest="outdir", default="", help="output file directory (local or PFN) (default = %default)")
        parser.add_option("--inpre", dest="inpre", default="", help="input file prefix (default = %default)")
        parser.add_option("--outpre", dest="outpre", default="", help="output file prefix (required) (default = %default)")
        parser.add_option("--fail", dest="fail", default=0, help="fail with specified error code (default = %default)")

    def checkExtraOptions(self,options,parser):
        super(jobSubmitterTest,self).checkExtraOptions(options,parser)

        if options.io:
            # input is not necessarily required
            if len(options.outdir)==0:
                parser.error("Required option: --outdir [directory]")
            if len(options.outpre)==0:
                parser.error("Required option: --outpre [prefix]")

    def generateSubmission(self):
        job = protoJob()
        job.name = self.name
        self.generatePerJob(job)

        for iJob in xrange(self.nParts):
            job.njobs += 1
            job.nums.append(iJob)

        job.queue = "-queue "+str(job.njobs)
        self.protoJobs.append(job)

    def generateExtra(self,job):
        super(jobSubmitterTest,self).generateExtra(job)
        job.patterns.update([
            ("JOBNAME",job.name+"_part$(Process)_$(Cluster)"),
            ("EXTRAINPUTS",""),
            ("EXTRAARGS","-j "+job.name+(" -i "+self.indir if len(self.indir)>0 else "")+" -o "+self.outdir+(" -n "+self.inpre if len(self.inpre)>0 else "")+" -u "+self.outpre+" -f "+str(self.fail)+" -p $(Process)" if self.io else ""),
        ])
