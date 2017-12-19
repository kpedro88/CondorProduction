import os, subprocess, sys, stat
from optparse import OptionParser
from collections import defaultdict, OrderedDict
from parseConfig import list_callback, parser_dict

# minimal sed-like function
# patterns = [(in,out),(in,out)]
# currently doesn't handle regex
def pysed(lines,out,patterns):
	with open(out,'w') as outfile:
		for line in lines:
			linetmp = line
			for pattern,replace in patterns.iteritems():
				linetmp = linetmp.replace(str(pattern),str(replace))
			outfile.write(linetmp)

class protoJob(object):
    def __init__(self):
        self.patterns = OrderedDict()
        self.appends = []
        self.queue = ""
        self.njobs = 0
        self.nums = []
        self.jdl = ""
        self.name = "job"
        
    def __repr__(self):
        line = (
            "protoJob:\n"
            "\tname = "+str(self.name)+"\n"
            "\tnjobs = "+str(self.njobs)+"\n"
            "\tjdl = "+str(self.jdl)+"\n"
            "\tqueue = "+str(self.queue)+"\n"
            "\tpatterns = "+str(self.patterns)+"\n"
            "\tappends = "+str(self.appends)+"\n"
            "\tnums = "+str(self.nums)
        )
        return line
        
    def makeName(self,num):
        return self.name+"_"+str(num)

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
        self.filesSet = set()
        self.runSet = set()
        self.missingLines = []

    def run(self):
        self.initRun()
        
        # job generation
        self.generateSubmission()
        
        # loop over protojobs
        for job in self.protoJobs:
            self.runPerJob(job)

        # final stuff
        self.finishRun()

    def initRun(self):
        self.initStep1()
        if self.missing:
            self.initMissing()
        
    def runPerJob(self,job):
        if self.prepare:
            self.doPrepare(job)

        # mutually exclusive
        if self.count:
            self.doCount(job)
        elif self.submit:
            self.doSubmit(job)
        elif self.missing:
            self.doMissing(job)
            
    def finishRun(self):
        if self.count:
            self.finishCount()
        elif self.missing:
            self.finishMissing()        
            
    def addDefaultOptions(self,parser):
        # control options
        parser.add_option("-c", "--count", dest="count", default=False, action="store_true", help="count the expected number of jobs (default = %default)")
        parser.add_option("-p", "--prepare", dest="prepare", default=False, action="store_true", help="prepare job inputs and JDL files (default = %default)")
        parser.add_option("-s", "--submit", dest="submit", default=False, action="store_true", help="submit jobs to condor (default = %default)")
        parser.add_option("-m", "--missing", dest="missing", default=False, action="store_true", help="check for missing jobs (default = %default)")
        parser.add_option("-r", "--resub", dest="resub", default="", help="make a resub script with specified name (default = %default)")
        parser.add_option("-u", "--user", dest="user", default=parser_dict["common"]["user"], help="view jobs from this user (submitter) (default = %default)")
        parser.add_option("-q", "--no-queue-arg", dest="noQueueArg", default=False, action="store_true", help="don't use -queue argument in condor_submit (default = %default)")

    def checkDefaultOptions(self,options,parser):
        if (options.submit + options.count + options.missing)>1:
            parser.error("Options -c, -s, -m are exclusive, pick one!")
        if (options.submit + options.count + options.missing + options.prepare)==0:
            parser.error("No operation mode selected! (-c, -p, -s, -m)")

    # if you use a different step1.sh, you might need to change these
    def addStep1Options(self,parser):
        self.defaultStep1 = True
        parser.add_option("-k", "--keep", dest="keep", default=False, action="store_true", help="keep existing tarball for job submission (default = %default)")
        parser.add_option("-n", "--no-voms", dest="novoms", default=False, action="store_true", help="skip check and use of voms proxy (default = %default)")
        parser.add_option("-t", "--cmssw-method", dest="cmsswMethod", type="choice", choices=["transfer","xrdcp","cmsrel"], default="transfer", help="how to get CMSSW env: transfer, xrdcp, or cmsrel (default = %default)")
        parser.add_option("-i", "--input", dest="input", default="", help="input dir for CMSSW tarball if using xrdcp (default = %default)")

    def checkStep1Options(self,options,parser):
        if options.cmsswMethod=="xrdcp" and len(options.input)==0:
            parser.error("CMSSW method xrdcp requires --input value")
        # no need to retar if not submitting or not using tarball
        if options.cmsswMethod=="cmsrel" or not options.submit:
            options.keep = True
        if options.novoms and options.cmsswMethod=="xrdcp":
            parser.error("Can't xrdcp CMSSW without voms proxy!")
            
    def addExtraOptions(self,parser):
        # job options
        parser.add_option("--jdl", dest="jdl", default="jobExecCondor.jdl", help="JDL template file for job (default = %default)")
        parser.add_option("--disk", dest="disk", default=10000000, help="specify amount of disk space per job [kB] (default = %default)")
        parser.add_option("--memory", dest="memory", default=2000, help="specify amount of memory per job [MB] (default = %default)")
        parser.add_option("--cpus", dest="cpus", default=1, help="specify number of CPU threads per job (default = %default)")
        parser.add_option("--sites", dest="sites", default=parser_dict["submit"]["sites"], help="comma-separated list of sites for global pool running (default = %default)")
        
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
        self.generateDefault(job)
        self.generateStep1(job)
        self.generateExtra(job)
        self.generateJdl(job)

    def initStep1(self):
        # check for grid proxy and tarball
        if self.defaultStep1:
            cmd = "./checkVomsTar.sh"
            if self.keep: cmd += " -k"
            if self.novoms: cmd += " -n"
            if not self.keep and self.cmsswMethod=="xrdcp": cmd += " -i "+self.input
            sp = subprocess.Popen(cmd, shell=True, stdin = sys.stdin, stdout = sys.stdout, stderr = sys.stderr)
            sp.wait()
            
    def initMissing(self):
        # find finished jobs via output file list
        self.filesSet = self.findFinished()
            
        # find running jobs from condor
        self.runSet = self.findRunning()
        
    def generateDefault(self,job):
        job.patterns["SCRIPTARGS"] = ",".join(self.scripts)

    def generateStep1(self,job):
        # command line args for step1
        cmsswver = os.getenv("CMSSW_VERSION")
        step1args = "-C "+cmsswver
        if self.cmsswMethod != "transfer":
            # xrdcp needs input dir, cmsrel needs scram arch
            step1args += " -L "+(self.input if self.cmsswMethod=="xrdcp" else os.getenv("SCRAM_ARCH"))
        job.patterns["STEP1ARGS"] = step1args
        if self.cmsswMethod=="transfer":
            job.patterns["CMSSWVER"] = cmsswver
        else:
            job.patterns["CMSSWVER.tar.gz, "] = ""
        if self.novoms:
            job.patterns["x509userproxy = $ENV(X509_USER_PROXY)\n"] = ""

    def generateExtra(self,job):
        job.patterns.update([
            ("MYDISK",self.disk),
            ("MYMEMORY",self.memory),
            ("MYCPUS",self.cpus),
        ])
        # special option for CMS Connect
        if os.uname()[1]=="login.uscms.org" and len(self.sites)>0:
            job.appends.append("+DESIRED_Sites = \""+self.sites+"\"")
        # left for the user: JOBNAME, EXTRAINPUTS, EXTRAARGS
        
    def generateJdl(self,job):
        job.jdl = self.jdl.replace(".jdl","_"+job.name+".jdl")
        
    def doCount(self,job):
        self.njobs += job.njobs
        
    def finishCount(self):
        print str(self.njobs)+" jobs"
        
    def doPrepare(self,job):
        # get template contents (move into separate fn/store in self?)
        if len(self.jdlLines)==0:
            with open(self.jdl,'r') as jdlfile:
                self.jdlLines = jdlfile.readlines()
        # replace patterns
        pysed(self.jdlLines,job.jdl,job.patterns)
        # append appends & queue
        with open(job.jdl,'a') as outfile:
            for append_ in job.appends:
                outfile.write(append_+"\n")
            if self.noQueueArg: outfile.write(job.queue.replace("-queue","Queue")+"\n")
            else: outfile.write("# "+job.queue.replace("-queue","Queue")+"\n")
                
    def doSubmit(self,job):
        if os.path.isfile(job.jdl):
#            cmd = "condor_submit "+job.jdl+" "+job.queue
            cmd = "condor_submit "+job.jdl
            os.system(cmd)
        else:
            print "Error: couldn't find "+job.jdl+", try running in prepare mode"
        
    def doMissing(self,job):
        jobSet, jobDict = self.findJobs(job)
        # find difference
        diffSet = jobSet - self.filesSet - self.runSet
        diffList = list(sorted(diffSet))
        if len(diffList)>0:
            if len(self.resub)>0:
                numlist = sorted([jobDict[j] for j in diffList])
                if self.noQueueArg:
                    # get jdl lines for this job
                    with open(job.jdl,'r') as file:
                        jdlLines = [line for line in file]
                    # overwrite queue command in jdl
                    with open(job.jdl,'w') as file:
                        for line in jdlLines:
                            if line.startswith("Queue"):
                                file.write("#"+line)
                                file.write("Queue Process in "+','.join(map(str,numlist))+"\n")
                            else:
                                file.write(line)
                    self.missingLines.append('condor_submit '+job.jdl+'\n')
                else:
                    self.missingLines.append('condor_submit '+job.jdl+' -queue Process in '+','.join(map(str,numlist))+'\n')
            else:
                self.missingLines.extend(diffList)

    def findJobs(self,job):
        jobSet = set()
        jobDict = {}
        for num in job.nums:
            name = job.makeName(num)
            jobSet.add(name)
            jobDict[name] = num
        return (jobSet, jobDict)
                
    def finishMissing(self):
        # provide results
        if len(self.missingLines)>0:
            if len(self.resub)>0:
                self.makeResubmit()
            else:
                print '\n'.join(self.missingLines)
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
            if "fnal.gov" in xrd and not "fnal.gov" in os.uname()[1]:
                # check for ticket
                test_krb = subprocess.Popen('klist', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                test_krb_msg = test_krb.communicate()
                has_krb = False
                for line in test_krb_msg[0].split('\n'):
                    if "Default principal" in line and "FNAL.GOV" in line:
                        has_krb = True
                        break
                if not has_krb:
                    raise RuntimeError("No kerberos ticket found for FNAL.GOV")
                files = filter(None,os.popen("ssh cmslpc-sl6.fnal.gov 'xrdfs "+xrd+" ls "+lfn+"'").read().split('\n'))
            else:
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
            global htcondor,classad
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
        
        constraint = ""
        if len(self.user)>0: constraint += 'Owner=="'+self.user+'"'
        for cname, collector in parser_dict["collectors"].iteritems():
            if len(collector)==0: continue
            if cname not in parser_dict["schedds"]:
                print "Error: no schedds provided for collector "+cname+", so it will be skipped."
            else:
                coll = htcondor.Collector(collector)
            for sch in parser_dict["schedds"][cname].split(','):
                try:
                    scheddAd = coll.locate(htcondor.DaemonTypes.Schedd, sch)
                    schedd = htcondor.Schedd(scheddAd)
                    for result in schedd.xquery(constraint,["Out"]):
                        runSet.add(self.runningToJobName(result["Out"]))
                except:
                    print "Warning: could not locate schedd "+sch
        
        return runSet
            
    def makeResubmit(self):
        with open(self.resub,'w') as rfile:
            rfile.write("#!/bin/bash\n\n")
            for stmp in self.missingLines:
                rfile.write(stmp+'\n')
        # make executable
        st = os.stat(rfile.name)
        os.chmod(rfile.name, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)            
        
