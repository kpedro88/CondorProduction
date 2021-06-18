from Condor.Production.jobSubmitter import *

class jobSubmitterTest(jobSubmitter):
    def __init__(self):
        super(jobSubmitterTest,self).__init__()
        self.scripts = ["test.sh"]

    def addExtraOptions(self,parser):
        super(jobSubmitterTest,self).addExtraOptions(parser)
        parser.add_option("-N", "--nParts", dest="nParts", default=1, type="int", help="number of parts to process (default = %default)")

    def generateSubmission(self):
        job = protoJob()
        job.name = "test"
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
            ("EXTRAARGS",""),
        ])
