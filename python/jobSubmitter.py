from __future__ import print_function
import os, subprocess, sys, stat, glob, shutil, tarfile, six
from optparse import OptionParser
from collections import defaultdict, OrderedDict
from datetime import datetime
from Condor.Production.parseConfig import list_callback, parser_dict

def date_convert(value):
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")

def date_callback(option, opt, value, parser):
    if value is None: return
    setattr(parser.values, option.dest, date_convert(value))

# minimal sed-like function
# patterns = [(in,out),(in,out)]
# currently doesn't handle regex
def pysed(lines,out,patterns):
    with open(out,'w') as outfile:
        for line in lines:
            linetmp = line
            for pattern,replace in six.iteritems(patterns):
                linetmp = linetmp.replace(str(pattern),str(replace))
            outfile.write(linetmp)

# run xrdfs ls using physical file name
def pyxrdfsls(pfn, minDate=None, maxDate=None):
    psplit = pfn.find("/store")
    lfn = pfn[psplit:]
    xrd = pfn[:psplit]
    checkDates = minDate is not None or maxDate is not None
    extra = " -l " if checkDates else ""

    # todo: replace w/ XRootD python bindings?
    results = filter(
        None,
        subprocess.Popen(
            "xrdfs "+xrd+" ls "+extra+lfn,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            # necessary to communicate w/ cmslpc at fnal
            env=dict(os.environ,**{'XrdSecGSISRVNAMES': 'cmseos.fnal.gov'})
        ).communicate()[0].split('\n')
    )

    if checkDates:
        dates = [date_convert(' '.join(line.split()[1:3])) for line in results]
        results = [line.split()[-1] for line,date in zip(results,dates) if (minDate is None or date>minDate) and (maxDate is None or date<maxDate)]

    return results

# run xrdcp
def pyxrdcp(a,b,verbose=True):
    # xrootd dir
    xrdcp = subprocess.Popen(
        "xrdcp "+a+" "+b,
        shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=dict(os.environ,**{'XrdSecGSISRVNAMES': 'cmseos.fnal.gov'})
    )
    xrdcp_result = xrdcp.communicate()
    rc = xrdcp.returncode
    if rc!=0 and verbose:
        print("exit code "+str(rc)+", failure in xrdcp")
        print(xrdcp_result[1])
    return rc

class protoJob(object):
    def __init__(self):
        self.patterns = OrderedDict()
        self.appends = []
        self.queue = ""
        self.njobs = 0
        self.nums = []
        self.jdl = ""
        self.name = "job"
        self.chainName = ""

    def __repr__(self):
        line = (
            "protoJob:\n"
            "\tname = "+str(self.name)+"\n"
            "\tchainName = "+str(self.chainName)+"\n"
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
        # dict of (string name, bool exclusive)
        self.modes = {}

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
        elif self.clean:
            self.initClean()

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
        elif self.clean:
            self.doClean(job)

    def finishRun(self):
        if self.count:
            self.finishCount()
        elif self.missing:
            self.finishMissing()
        elif self.clean:
            self.finishClean()

    def addDefaultOptions(self,parser):
        # control options
        parser.add_option("-c", "--count", dest="count", default=False, action="store_true", help="count the expected number of jobs (default = %default)")
        parser.add_option("-p", "--prepare", dest="prepare", default=False, action="store_true", help="prepare job inputs and JDL files (default = %default)")
        parser.add_option("-s", "--submit", dest="submit", default=False, action="store_true", help="submit jobs to condor (default = %default)")
        parser.add_option("-m", "--missing", dest="missing", default=False, action="store_true", help="check for missing jobs (default = %default)")
        parser.add_option("-r", "--resub", dest="resub", default="", help="make a resub script with specified name (default = %default)")
        parser.add_option("--min-date", dest="minDate", type="string", default=None, action="callback", callback=date_callback, help="minimum date for files in missing mode (default = %default)")
        parser.add_option("--max-date", dest="maxDate", type="string", default=None, action="callback", callback=date_callback, help="maximum date for files in missing mode (default = %default)")
        parser.add_option("-l", "--clean", dest="clean", default=False, action="store_true", help="clean up log files (default = %default)")
        parser.add_option("--clean-dir", dest="cleanDir", default=".", help="output dir for log file .tar.gz (default = %default)")
        parser.add_option("-u", "--user", dest="user", default=parser_dict["common"]["user"], help="view jobs from this user (submitter) (default = %default)")
        parser.add_option("-q", "--no-queue-arg", dest="noQueueArg", default=False, action="store_true", help="don't use -queue argument in condor_submit (default = %default)")
        self.modes.update({
            "count": 1,
            "prepare": 0,
            "submit": 1,
            "missing": 1,
            "clean": 1,
        })

    def checkDefaultOptions(self,options,parser):
        nModes = 0
        nExcls = 0
        lModes = ""
        lExcls = ""
        for mode,excl in six.iteritems(self.modes):
            lModes += mode + ", "
            if excl: lExcls += mode + ", "
            if getattr(options,mode):
                nModes += 1
                if excl: nExcls += 1
        lModes = lModes[:-2]
        lExcls = lExcls[:-2]
        if nExcls>1:
            parser.error("Modes "+lExcls+" are exclusive, pick one!")
        if nModes==0:
            parser.error("No operation mode selected! ("+lModes+")")

    # if you use a different step1.sh, you might need to change these
    def addStep1Options(self,parser):
        self.defaultStep1 = True
        parser.add_option("-k", "--keep", dest="keep", default=False, action="store_true", help="keep existing tarball for job submission (default = %default)")
        parser.add_option("-n", "--no-voms", dest="novoms", default=False, action="store_true", help="skip check and use of voms proxy (default = %default)")
        parser.add_option("-t", "--cmssw-method", dest="cmsswMethod", default="transfer", help="how to get CMSSW env: transfer, cmsrel, or address for xrdcp (default = %default)")
        parser.add_option("--scram-workaround", dest="scramWorkaround", default=False, action="store_true", help="workaround for scram ProjectRename bug (default = %default)")

    def checkStep1Options(self,options,parser):
        if options.cmsswMethod!="transfer" and options.cmsswMethod!="cmsrel" and not options.cmsswMethod.startswith("root://"):
            parser.error("Unknown CMSSW method: "+options.cmsswMethod)
        # no need to retar if not submitting or not using tarball
        if options.cmsswMethod=="cmsrel" or not options.submit:
            options.keep = True
        if options.novoms and options.cmsswMethod.startswith("root://"):
            parser.error("Can't xrdcp CMSSW without voms proxy!")

    def addExtraOptions(self,parser):
        # job options
        parser.add_option("--jdl", dest="jdl", default="jobExecCondor.jdl", help="JDL template file for job (default = %default)")
        parser.add_option("--disk", dest="disk", default=1000000, help="specify amount of disk space per job [kB] (default = %default)")
        parser.add_option("--memory", dest="memory", default=2000, help="specify amount of memory per job [MB] (default = %default)")
        parser.add_option("--cpus", dest="cpus", default=1, help="specify number of CPU threads per job (default = %default)")
        parser.add_option("--sites", dest="sites", default=parser_dict["submit"]["sites"], help="comma-separated list of sites for global pool running (default = %default)")
        parser.add_option("--env", dest="env", default=None, help="args to run job in Singularity environment using cmssw-env (default = %default)")
        parser.add_option("--intermediate", dest="intermediate", default=False, action="store_true", help="specify that this is an intermediate job in a chain to disable staging out (default = %default)")
        parser.add_option("--singularity", dest="singularity", default="", help="specify singularity image for job (default = %default)")

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
            if not self.keep and self.cmsswMethod.startswith("root://"): cmd += " -i "+self.cmsswMethod
            sp = subprocess.Popen(cmd, shell=True, stdin = sys.stdin, stdout = sys.stdout, stderr = sys.stderr)
            sp.wait()

    def initMissing(self):
        # find finished jobs via output file list
        self.filesSet = self.findFinished()

        # find running jobs from condor
        self.runSet = self.findRunning()

    def initClean(self):
        self.initMissing()
        # subtract running jobs from finished jobs (in case resubmitted)
        self.filesSet = self.filesSet - self.runSet

        self.logdir = "logs"
        if not os.path.isdir(self.logdir):
            os.mkdir(self.logdir)

    def generateDefault(self,job):
        job.patterns["SCRIPTARGS"] = ",".join(self.scripts)

    def generateStep1(self,job):
        # command line args for step1
        cmsswver = os.getenv("CMSSW_VERSION")
        step1args = "-C "+cmsswver
        if self.cmsswMethod == "cmsrel" or self.scramWorkaround:
            # cmsrel needs scram arch
            step1args += " -L "+os.getenv("SCRAM_ARCH")
        if self.cmsswMethod.startswith("root://"):
            # xrdcp needs input dir
            step1args += " -X "+self.cmsswMethod
        job.patterns["STEP1ARGS"] = step1args
        if self.cmsswMethod=="transfer":
            job.patterns["CMSSWVER"] = cmsswver
        else:
            job.patterns["CMSSWVER.tar.gz, "] = ""
        if self.novoms:
            job.patterns["x509userproxy = $ENV(X509_USER_PROXY)\n"] = ""

    def generateExtra(self,job):
        is_cms_connect = os.uname()[1]=="login.uscms.org" or os.uname()[1]=="login-el7.uscms.org"
        job.patterns.update([
            ("OSVERSION","rhel7" if "el7" in os.uname()[2] else "rhel6"),
            ("MYDISK",self.disk),
            ("MYMEMORY",self.memory),
            ("MYCPUS",self.cpus),
            ("ENVARGS",("-E {}".format(self.env) if self.env is not None else "")+(" -I" if self.intermediate else "")),
            ("SINGULARITYARGS",('+SingularityImage = "{}"'.format(self.singularity) if len(self.singularity)>0 else "")+("\nRequirements = HAS_SINGULARITY == True" if is_cms_connect else "")),
        ])
        # special option for CMS Connect
        if is_cms_connect:
            if len(self.sites)>0: job.appends.append("+DESIRED_Sites = \""+self.sites+"\"")
            job.appends.append("+AvoidSystemPeriodicRemove = True")
        # special option for UMD
        if "umd.edu" in os.uname()[1]:
            if 'slc7' in os.environ['SCRAM_ARCH']:
                job.appends.append("Requirements = (TARGET.OpSysMajorVer == 7)")
            else:
                job.appends.append("Requirements = (TARGET.OpSysMajorVer == 6)")
        # left for the user: JOBNAME, EXTRAINPUTS, EXTRAARGS

    def generateJdl(self,job):
        job.jdl = self.jdl.replace(".jdl","_"+job.name+".jdl")

    def doCount(self,job):
        self.njobs += job.njobs

    def finishCount(self):
        print(str(self.njobs)+" jobs")

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
            if self.noQueueArg:
                cmd = "condor_submit "+job.jdl
            else:
                queue = job.queue
                # form should be: '-queue "..."'
                if queue[7]!='"': queue = queue[:7]+'"'+queue[7:]
                if queue[-1]!='"': queue = queue+'"'
                cmd = "condor_submit "+job.jdl+" "+job.queue
            os.system(cmd)
        else:
            print("Error: couldn't find "+job.jdl+", try running in prepare mode")

    def doMissing(self,job):
        jobSet, jobDict = self.findJobs(job)
        # replace name if necessary
        if len(job.chainName)>0:
            runSetTmp = {x.replace(job.chainName,job.name) for x in self.runSet}
        else:
            runSetTmp = self.runSet
        # find difference
        diffSet = jobSet - self.filesSet - runSetTmp
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
                    self.missingLines.append('condor_submit '+job.jdl)
                else:
                    self.missingLines.append('condor_submit '+job.jdl+' -queue "Process in '+','.join(map(str,numlist))+'"')
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
                print('\n'.join(self.missingLines))
        else:
            print("No missing jobs!")

    def finishedToJobName(self,val):
        return val.split("/")[-1].replace(".root","")

    def findFinished(self):
        # find finished jobs via output file list
        filesSet = set()
        if hasattr(self,"output"):
            files = pyxrdfsls(self.output,self.minDate,self.maxDate)
            # basename
            filesSet = set([ self.finishedToJobName(f) for f in files])
        return filesSet

    def tryToGetCondor(self):
        # try to find condor bindings
        for condorPath in ["/usr/lib64/python2.6/site-packages", "/usr/lib64/python2.7/site-packages"]:
            if condorPath not in sys.path and os.path.isdir(condorPath):
                sys.path.append(condorPath)
        # try to import condor bindings
        try:
            global htcondor,classad
            import htcondor,classad
        except:
            print('Could not import htcondor bindings!')
            return False
        return True

    def runningToJobName(self,val):
        return "_".join(val.replace(".stdout","").split('_')[:-1])

    def findRunning(self):
        runSet = set()

        hasCondor = self.tryToGetCondor()
        if not hasCondor:
            print('"Missing jobs" check will not consider running jobs.')
            return runSet

        # exclude removed jobs
        constraint = "JobStatus!=3"
        if len(self.user)>0: constraint += ' && Owner=="'+self.user+'"'
        for cname, collector in six.iteritems(parser_dict["collectors"]):
            if cname not in parser_dict["schedds"]:
                print("Error: no schedds provided for collector "+cname+", so it will be skipped.")
                continue
            else:
                if len(collector)==0:
                    coll = htcondor.Collector()
                else:
                    coll = htcondor.Collector(collector)
            for sch in parser_dict["schedds"][cname].split(','):
                try:
                    scheddAd = coll.locate(htcondor.DaemonTypes.Schedd, sch)
                    schedd = htcondor.Schedd(scheddAd)
                    for result in schedd.xquery(constraint,["Out"]):
                        runSet.add(self.runningToJobName(result["Out"]))
                except:
                    print("Warning: could not locate schedd "+sch)

        return runSet

    def makeResubmit(self):
        with open(self.resub,'w') as rfile:
            rfile.write("#!/bin/bash\n\n")
            for stmp in self.missingLines:
                rfile.write(stmp+'\n')
        # make executable
        st = os.stat(rfile.name)
        os.chmod(rfile.name, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    def doClean(self,job):
        jobSet, jobDict = self.findJobs(job)

        finishedJobSet = jobSet & self.filesSet
        # remove these jobs from global list
        self.filesSet = self.filesSet - jobSet

        for jobname in finishedJobSet:
            # gets .condor, .stdout, .stderr
            for fname in glob.glob(jobname+"_*.*"):
                shutil.move(fname,self.logdir)

    def finishClean(self):
        # check if nothing to do
        if len(os.listdir(self.logdir))==0:
            # remove tmp dir
            shutil.rmtree(self.logdir)
            return

        # create compressed tarfile
        logname = "logs_tmp.tar.gz"
        with tarfile.open(logname,"w:gz") as tar:
            tar.add(self.logdir,arcname=self.logdir)

        num_logs = 0
        # check what is already in dir
        if len(self.cleanDir)==0: self.cleanDir = "."
        if self.cleanDir.startswith("root://"):
            # xrootd dir
            files = pyxrdfsls(self.cleanDir)
            files = [f for f in files if f.endswith(".tar.gz")]
        else:
            # local dir
            files = glob.glob(logname.replace("tmp","*"))

        files = [f for f in files if f!=logname]
        if len(files)>0:
            num_logs = max([int(f.split("_")[-1].replace(".tar.gz","")) for f in files])+1
        logname2 = logname.replace("tmp",str(num_logs))

        rc = 1
        # copy to dir
        if self.cleanDir.startswith("root://"):
            # xrootd dir
            rc = pyxrdcp(logname,self.cleanDir+"/"+logname2)
        else:
            # local dir
            shutil.copy2(logname,self.cleanDir+"/"+logname2)
            rc = 0

        if rc==0:
            print("copied logs to "+self.cleanDir+"/"+logname2)
            # remove tmp file
            os.remove(logname)
            # remove tmp dir
            shutil.rmtree(self.logdir)
