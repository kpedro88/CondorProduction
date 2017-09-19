import os, subprocess, sys, stat
from optparse import OptionParser
from collections import defaultdict
from parseConfig import list_callback, parser_dict

# todo: check path, try/except, move to where used in missing functionality?
#import htcondor,classad

# minimal sed-like function
# patterns = [(in,out),(in,out)]
# currently doesn't handle regex
def pysed(lines,out,patterns):
	with open(out,'w') as outfile:
		for line in lines:
			linetmp = line
			for pattern in patterns:
				linetmp = line.replace(pattern[0],pattern[1])
			outfile.write(linetmp)

class protoJob(object):
    def __init__():
        self.patterns = []
        self.appends = []
        self.queue = ""
        self.njobs = 0
        self.nums = []
        self.names = []
        self.jdl = ""
        self.name = "job"

class jobSubmitter(object):
    def __init__(self,argv=None,parser=None):
        if argv is None: argv = sys.argv[1:]
        
        self.defaultStep1 = False
        self.scripts = ["step1.sh","step2.sh"]
        
        # define parser
        if parser is None:
            parser = OptionParser(add_help_option=False)
            parser.add_option("--help", dest="help", action="store_true", default=False, help='show this help message')
        # add sets of options
        self.addDefaultOptions(parser)
        self.addStep1Options(parser)
        self.addExtraOptions(parser)
        # parse & do help
        (options, args) = parser.parse_args(args=argv)
        if options.help:
           parser.print_help()
           sys.exit()
        # check for option errors
        self.checkDefaultOptions(options,parser)
        self.checkStep1Options(options,parser)
        self.checkExtraOptions(options,parser)
        # set as members
        for key in options.__dict__:
            setattr(self,key,options.__dict__[key])
        
        # other vars
        self.protoJobs = []
        self.jdlLines = []
        self.njobs = 0
        self.jobSet = set()
        self.jobRef = {}

    def run(self):
        self.initStep1()
            
        # job generation
        self.generateSubmission()
        
        # loop over protojobs
        for job in self.protoJobs:
            if self.count:
                self.doCount(job)
            elif self.prepare:
                self.doPrepare(job)
                if self.submit:
                    self.doSubmit(job)
            elif self.missing:
                self.doMissing(job)
        
        if self.count:
            self.finishCount()
        elif self.missing:
            self.finishMissing()

    def addDefaultOptions(self,parser):
        # control options
        parser.add_option("-c", "--count", dest="count", default=False, action="store_true", help="count the expected number of jobs (default = %default)")
        parser.add_option("-p", "--prepare", dest="prepare", default=False, action="store_true", help="prepare job inputs and JDL files (default = %default)")
        parser.add_option("-s", "--submit", dest="submit", default=False, action="store_true", help="submit jobs to condor once they are configured (default = %default)")
        parser.add_option("-m", "--missing", dest="missing", default=False, action="store_true", help="check for missing jobs (default = %default)")
        parser.add_option("-r", "--resub", dest="resub", default="", help="make a resub script with specified name (default = %default)")
        parser.add_option("-u", "--user", dest="user", default=parser_dict["common"]["user"], help="view jobs from this user (submitter) (default = %default)")

    def checkDefaultOptions(self,options,parser):
        if (options.submit + options.count + options.missing)>1:
            parser.error("Options -c, -s, -m are exclusive, pick one!")
        if (options.submit + options.count + options.missing + options.prepare)==0:
            parser.error("No operation mode selected! (-c, -p, -s, -m)")
        # submit demands prepare
        if options.submit and not options.prepare:
            options.prepare = True

    # if you use a different step1.sh, you might need to change these
    def addStep1Options(self,parser):
        self.defaultStep1 = True
        parser.add_option("-k", "--keep", dest="keep", default=False, action="store_true", help="keep existing tarball for job submission (default = %default)")
        parser.add_option("-t", "--cmssw-method", dest="cmsswMethod", type="choice", choices=["transfer","xrdcp","cmsrel"], default="transfer", help="how to get CMSSW env: transfer, xrdcp, or cmsrel (default = %default)")
        parser.add_option("-i", "--input", dest="input", default="", help="input dir for CMSSW tarball if using xrdcp (default = %default)")

    def checkStep1Options(self,options,parser):
        if options.cmsswMethod=="xrdcp" and len(options.input)==0:
            parser.error("CMSSW method xrdcp requires --input value")
        # no need to retar if not submitting or not using tarball
        if options.cmsswMethod=="cmsrel" or not options.submit:
            options.keep = True
            
    def addExtraOptions(self,parser):
        # job options
        parser.add_option("--jdl", dest="jdl", default="jobExecCondor.jdl", help="JDL template file for job (default = %default)")
        parser.add_option("--disk", dest="disk", default=10000000, help="specify amount of disk space per job [kB] (default = %default)")
        parser.add_option("--memory", dest="memory", default=2000, help="specify amount of memory per job [MB] (default = %default)")
        parser.add_option("--cpus", dest="cpus", default=1, help="specify number of CPU threads per job (default = %default)")
        parser.add_option("--sites", dest="sites", default="", help="comma-separated list of sites for global pool running (default = %default)")
        
    def checkExtraOptions(self,options,parser):
        pass
        
    # in case you want to keep most but not all options from a section
    def removeOptions(self,parser,*options):
        for option in options:
            if parser.has_option(option):
                parser.remove_option(option)

    def generateSubmission(self):
        pass
        
    def generatePerJob(self,job):
        self.generateDefault(self,job)
        self.generateStep1(self,job)
        self.generateExtra(self,job)

    def initStep1(self):
        # check for grid proxy and tarball
        if self.defaultStep1:
            cmd = "./checkVomsTar.sh"
            if self.keep: cmd += " -k"
            if not self.keep and self.cmsswMethod=="xrdcp": cmd += " -i "+self.input
            sp = subprocess.Popen(cmd, shell=True, stdin = sys.stdin, stdout = sys.stdout, stderr = sys.stderr)
            sp.wait()
        
    def generateDefault(self,job):
        job.patterns.append(("SCRIPTARGS",",".join(self.scripts)))

    def generateStep1(self,job):
        # command line args for step1
        step1args = "-C "+os.environ("CMSSW_VERSION")
        if self.cmsswMethod != "transfer":
            # xrdcp needs input dir, cmsrel needs scram arch
            step1args += " -L "+(self.input if self.cmsswMethod=="xrdcp" else os.environ("SCRAM_ARCH"))
        job.patterns.append(("STEP1ARGS",step1args))

    def generateExtra(self,job):
        job.patterns.extend([
            ("MYDISK",self.disk),
            ("MYMEMORY",self.memory),
            ("MYCPUS",self.cpus),
        ])
        # special option for CMS Connect
        if os.uname()[1]=="login.uscms.org" and len(self.sites)>0:
            job.appends.append("+DESIRED_Sites = \""+options.sites+"\"")
        # left for the user: JOBNAME, EXTRAINPUTS, EXTRAARGS
        
    def doCount(self,job):
        self.njobs += job.njobs
        
    def finishCount(self):
        print str(self.njobs)+" jobs"
        
    def doPrepare(self,job):
        # get template contents (move into separate fn/store in self?)
        if len(self.jdlLines)==0:
            with open(self.jdl,'r') as jdlfile:
                self.jdlLines = jdlfile.read_lines()
        job.jdl = self.jdl.replace(".jdl","_"+job.name+".jdl")
        # replace patterns
        pysed(self.jdlLines,job.jdl,job.patterns)
        # append appends & queue
        with open(job.jdl,'a') as outfile:
            for append_ in job.appends:
                outfile.write(append_+"\n")
            outfile.write("# "+job.queue.replace("-queue","Queue")+"\n")
                
    def doSubmit(self,job):
        cmd = "condor_submit "+job.jdl+" "+job.queue
        os.system(cmd)
        
    def doMissing(self,job):
        self.jobSet.update(job.names)
        for j in job.names:
            self.jobRef[j] = job

    def finishMissing(self):
        # find finished jobs via output file list
        filesSet = self.findFinished()
            
        # find running jobs from condor
        runSet = self.findRunning()

        # find difference
        diffSet = self.jobSet - filesSet - runSet
        diffList = list(sorted(diffSet))
        
        # provide results
        if len(diffList)>0:
            if len(self.resub)>0:
                makeResubmit(diffList)
            else:
                print '\n'.join(diffList)
        else:
            print "No missing jobs!"
            
    def finishedToJobName(self,val):
        return val.split("/")[-1].replace(".root","")
        
    def findFinished(self):
        # find finished jobs via output file list
        filesSet = set()
        if hasattr(self,"output"):
            outsplit = self.output.find("/store")
            lfn = self.output[outsplit:]
            xrd = self.output[:outsplit]
            files = filter(None,os.popen("xrdfs "+xrd+" ls "+lfn).read().split('\n'))
            # basename
            filesSet = set([ self.finishedToJobName(f) for f in files])
        return filesSet

    def tryToGetCondor(self):
        # try to find condor bindings
        condorPath = "/usr/lib64/python2.6/site-packages"
        if condorPath not in sys.path and os.path.isdir(condorPath):
            sys.path.append(condorPath)
        # try to import condor bindings
        try:
            import htcondor,classad
        except:
            print 'Could not import htcondor bindings!'
            return False
        return True

    def runningToJobName(self,val):
        return "_".join(val.replace(".stdout","").split('_')[:-1])
        
    def findRunning(self):
        runSet = set()
    
        hasCondor = self.tryToGetCondor()
        if not hasCondor:
            print '"Missing jobs" check will not consider running jobs.'
            return runSet
        
        from collectors import collectors
        constraint = ""
        if len(self.user)>0: constraint += 'Owner=="'+self.user+'"'
        for collector in collectors:
            coll = htcondor.Collector(collector[0])
            for sch in collector[1]:
                scheddAd = coll.locate(htcondor.DaemonTypes.Schedd, sch)
                schedd = htcondor.Schedd(scheddAd)
                for result in schedd.xquery(constraint,["Out"]):
                    runSet.add(runningToJobName(result["Out"]))
        
        return runSet
            
    def makeResubmit(self,diffList):
        with open(self.resub,'w') as rfile:
            rfile.write("#!/bin/bash\n\n")
            diffDict = defaultdict(list)
            for dtmp in diffList:
                stmp = self.jobRef[dtmp].jdl
                ntmp = dtmp.split('_')[-1]
                diffDict[stmp].append(ntmp)
            for stmp in sorted(diffDict):
                rfile.write('condor_submit '+stmp+' -queue Process in '+','.join(diffDict[stmp])+'\n')
        # make executable
        st = os.stat(rfile.name)
        os.chmod(rfile.name, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)            
        
