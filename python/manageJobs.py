import sys,os,subprocess,glob,shutil,json
from optparse import OptionParser

# try to find condor bindings
condorPath = "/usr/lib64/python2.6/site-packages"
if condorPath not in sys.path and os.path.isdir(condorPath):
    sys.path.append(condorPath)
import htcondor,classad

has_paramiko = True
try:
    import paramiko
except:
    has_paramiko = False
try:
    import gssapi
except:
    has_paramiko = False

from parseConfig import list_callback, parser_dict

class CondorJob(object):
    def __init__(self, result, schedd):
        self.stdout = result["Out"].replace(".stdout","")
        self.name = "_".join(self.stdout.split('_')[:-1])
        self.num = str(result["ClusterId"])+"."+str(result["ProcId"])
        self.schedd = schedd
        self.why = result["HoldReason"] if "HoldReason" in result.keys() else ""
        self.args = result["Args"]
        self.status = int(result["JobStatus"]) # 2 is running, 5 is held, 1 is idle
        self.sites = result["DESIRED_Sites"] if "DESIRED_Sites" in result.keys() else ""
        if self.sites==classad.Value.Undefined: self.sites = ""
        self.matched = result["MATCH_EXP_JOB_GLIDEIN_CMSSite"] if "MATCH_EXP_JOB_GLIDEIN_CMSSite" in result.keys() else ""
        if self.matched==classad.Value.Undefined: self.matched = ""
        if "RemoteHost" in result.keys():
            self.machine = result["RemoteHost"]
        elif "LastRemoteHost" in result.keys():
            self.machine = result["LastRemoteHost"]
        else:
            self.machine = ""
        if self.machine==classad.Value.Undefined: self.matched = ""
        if len(self.machine)>0: self.machine = self.machine.split('@')[-1]
        self.time = (float(result["ChirpCMSSWElapsed"]) if "ChirpCMSSWElapsed" in result.keys() else 0.0)/float(3600)
        self.events = int(result["ChirpCMSSWEvents"]) if "ChirpCMSSWEvents" in result.keys() else 0
        self.rate = float(self.events)/(self.time*3600) if self.time>0 else 0

def getJob(options,result,jobs,scheddurl=""):
    # check greps
    checkstring = result["Out"]
    if "HoldReason" in result.keys(): checkstring += " "+result["HoldReason"]
    gfound = False
    for gcheck in options.grep:
        if gcheck in checkstring:
            gfound = True
            break
    if len(options.grep)>0 and not gfound: return
    vfound = False
    for vcheck in options.vgrep:
        if vcheck in checkstring:
            vfound = True
            break
    if len(options.vgrep)>0 and vfound: return
    if options.stuck:
        time = int(result["ServerTime"]) if "ServerTime" in result.keys() else 0
        update = int(result["ChirpCMSSWLastUpdate"]) if "ChirpCMSSWLastUpdate" in result.keys() else 0
        # look for jobs not updating for 12 hours
        tdiff = time - update
        if time>0 and update>0 and tdiff>(options.stuckThreshold*3600): result["HoldReason"] = "Job stuck for "+str(tdiff/3600)+" hours"
        else: return
    jobs.append(CondorJob(result,scheddurl))

def getSchedd(scheddurl,coll=""):
    if len(scheddurl)>0:
        try:
            if len(coll)>0: coll = htcondor.Collector(coll)
            else: coll = htcondor.Collector() # defaults to local
            scheddAd = coll.locate(htcondor.DaemonTypes.Schedd, scheddurl)
            schedd = htcondor.Schedd(scheddAd)
        except:
            print "Warning: could not locate schedd "+scheddurl
            return None
    else:
        schedd = htcondor.Schedd() # defaults to local
    return schedd

def getJobs(options, scheddurl=""):
    constraint = 'Owner=="'+options.user+'"'
    if options.held: constraint += ' && JobStatus==5'
    elif options.running: constraint += ' && JobStatus==2'
    elif options.idle: constraint += ' && JobStatus==1'

    schedd = getSchedd(scheddurl,options.coll)

    # get info for selected jobs
    jobs = []
    props = ["ClusterId","ProcId","HoldReason","Out","Args","JobStatus","ServerTime","ChirpCMSSWLastUpdate","ChirpCMSSWElapsed","ChirpCMSSWEvents","DESIRED_Sites","MATCH_EXP_JOB_GLIDEIN_CMSSite","RemoteHost","LastRemoteHost"]
    if options.finished>0:
        for result in schedd.history(constraint,props,options.finished):
            getJob(options,result,jobs,scheddurl)
    else:
        for result in schedd.xquery(constraint,props):
            getJob(options,result,jobs,scheddurl)
    return jobs

def printJobs(jobs, num=False, prog=False, stdout=False, why=False, matched=False):
    if len(jobs)==0: return
    
    print "\n".join([
        (j.stdout if stdout else j.name)+
        (" ("+j.num+")" if num else "")+
        (" ({:d} events in {:.1f} hours = {:.1f} evt/sec)".format(j.events,j.time,j.rate) if prog else "")+
        (" : "+j.matched+", "+j.machine if matched and len(j.matched)>0 and len(j.machine)>0 else "")+
        (" : "+j.why if why and len(j.why)>0 else "")
        for j in jobs
    ])

def resubmitJobs(jobs,options,scheddurl=""):
    # get scheduler
    schedd = getSchedd(scheddurl,options.coll)
    # process edits from JSON into dict
    edits = {}
    if len(options.edit)>0:
        try:
            edits = json.loads(options.edit)
        except:
            print "edit not specified in JSON format! Exiting."
            sys.exit(1)
    # create backup dir if desired
    backup_dir = ""
    tmp_dir = ""
    if len(options.dir)>0:
        backup_dir = options.dir+"/backup"
        if not os.path.isdir(backup_dir):
            os.mkdir(backup_dir)
        tmp_dir = options.dir+"/tmp"
        if not os.path.isdir(tmp_dir):
            os.mkdir(tmp_dir)
    # actions that must be done per-job
    for j in jobs:
        logfile = options.dir+"/"+j.stdout+".stdout"
        # hold running jobs first (in case hung)
        if j.status==2:
            if len(options.dir)>0:
                logfile = tmp_dir+"/"+j.stdout+".stdout"
                # generate a backup log from condor_tail
                cmdt = "condor_tail -maxbytes 10000000 "+j.num
                with open(logfile,'w') as logf:
                    subprocess.Popen(cmdt, shell=True, stdout=logf, stderr=subprocess.PIPE).communicate()
            schedd.act(htcondor.JobAction.Hold,[j.num])
        # backup log
        if len(options.dir)>0 and not options.idle:
            prev_logs = glob.glob(backup_dir+"/"+j.stdout+"_*")
            num_logs = 0
            # increment log number if job has been resubmitted before
            if len(prev_logs)>0:
                num_logs = max([int(log.split("_")[-1].replace(".stdout","")) for log in prev_logs])+1
            # copy logfile
            if os.path.isfile(logfile):
                shutil.copy2(logfile,backup_dir+"/"+j.stdout+"_"+str(num_logs)+".stdout")
        # edit redirector
        if len(options.xrootd)>0:
            args = j.args.split(' ')
            args = [a.replace('"','').rstrip() for a in args]
            # assumption: "-x" argument used for redirector
            try:
                args[args.index("-x")+1] = options.xrootd
            except:
                args.extend(["-x",options.xrootd])
            schedd.edit([j.num],"Args",'"'+" ".join(args[:])+'"')
    # actions that can be applied to all jobs
    jobnums = [j.num for j in jobs]
    # reset counts to avoid removal
    schedd.edit(jobnums,"NumShadowStarts","0")
    schedd.edit(jobnums,"NumJobStarts","0")
    schedd.edit(jobnums,"JobRunCount","0")
    # change sites if desired
    # takes site list from the first job
    if len(options.addsites)>0 or len(options.rmsites)>0:
        sitelist = filter(None,jobs[0].sites.split(','))
        for addsite in options.addsites:
            if not addsite in sitelist: sitelist.append(addsite)
        for rmsite in options.rmsites:
            if rmsite in sitelist: del sitelist[sitelist.index(rmsite)]
        schedd.edit(jobnums,"DESIRED_Sites",'"'+','.join(sitelist)+'"')
    # any other classad edits
    for editname,editval in edits.iteritems():
        schedd.edit(jobnums,str(editname),str(editval))
    # release jobs (unless idle - then no need to release)
    if not options.idle:
        schedd.act(htcondor.JobAction.Release,jobnums)

def manageJobs(argv=None):
    if argv is None: argv = sys.argv[1:]
    
    parser = OptionParser(add_help_option=False)
    parser.add_option("-c", "--coll", dest="coll", default="", help="view jobs from this collector (default = %default)")
    parser.add_option("-u", "--user", dest="user", default=parser_dict["common"]["user"], help="view jobs from this user (submitter) (default = %default)")
    parser.add_option("-a", "--all", dest="all", default=False, action="store_true", help="view jobs from all schedulers (default = %default)")
    parser.add_option("-h", "--held", dest="held", default=False, action="store_true", help="view only held jobs (default = %default)")
    parser.add_option("-r", "--running", dest="running", default=False, action="store_true", help="view only running jobs (default = %default)")
    parser.add_option("-i", "--idle", dest="idle", default=False, action="store_true", help="view only idle jobs (default = %default)")
    parser.add_option("-f", "--finished", dest="finished", default=0, type=int, help="view only n finished jobs  (default = %default)")
    parser.add_option("-t", "--stuck", dest="stuck", default=False, action="store_true", help="view only stuck jobs (subset of running) (default = %default)")
    parser.add_option("-g", "--grep", dest="grep", default=[], type="string", action="callback", callback=list_callback, help="view jobs with [comma-separated list of strings] in the job name or hold reason (default = %default)")
    parser.add_option("-v", "--vgrep", dest="vgrep", default=[], type="string", action="callback", callback=list_callback, help="view jobs without [comma-separated list of strings] in the job name or hold reason (default = %default)")
    parser.add_option("-o", "--stdout", dest="stdout", default=False, action="store_true", help="print stdout filenames instead of job names (default = %default)")
    parser.add_option("-n", "--num", dest="num", default=False, action="store_true", help="print job numbers along with names (default = %default)")
    parser.add_option("-x", "--xrootd", dest="xrootd", default="", help="edit the xrootd redirector (or site name) of the job input (default = %default)")
    parser.add_option("-e", "--edit", dest="edit", default="", help="edit the ClassAds of the job (JSON dict format) (default = %default)")
    parser.add_option("-s", "--resubmit", dest="resubmit", default=False, action="store_true", help="resubmit the selected jobs (default = %default)")
    parser.add_option("-k", "--kill", dest="kill", default=False, action="store_true", help="remove the selected jobs (default = %default)")
    parser.add_option("-d", "--dir", dest="dir", default=parser_dict["manage"]["dir"], help="directory for stdout files (used for backup when resubmitting) (default = %default)")
    parser.add_option("-w", "--why", dest="why", default=False, action="store_true", help="show why a job was held (default = %default)")
    parser.add_option("-m", "--matched", dest="matched", default=False, action="store_true", help="show site and machine to which the job matched (default = %default)")
    parser.add_option("-p", "--progress", dest="progress", default=False, action="store_true", help="show job progress (time and nevents) (default = %default)")
    parser.add_option("-R", "--remote", dest="remote", default=False, action="store_true", help="access remote schedds (default = %default)")
    parser.add_option("--add-sites", dest="addsites", default=[], type="string", action="callback", callback=list_callback, help='comma-separated list of global pool sites to add (default = %default)')
    parser.add_option("--rm-sites", dest="rmsites", default=[], type="string", action="callback", callback=list_callback, help='comma-separated list of global pool sites to remove (default = %default)')
    parser.add_option("--stuck-threshold", dest="stuckThreshold", default=12, help="threshold in hours to define stuck jobs (default = %default)")
    parser.add_option("--ssh", dest="ssh", action="store_true", default=False, help='internal option if script is run recursively over ssh')
    parser.add_option("--help", dest="help", action="store_true", default=False, help='show this help message')
    (options, args) = parser.parse_args(args=argv)

    if options.help:
       parser.print_help()
       sys.exit()

    uname = os.uname()

    # check for exclusive options
    if options.stuck:
        options.running = True
    if (options.held + options.running + options.idle + int(options.finished>0))>1:
        parser.error("Options -h, -r, -i, -f are exclusive, pick one!")
    if options.resubmit and options.kill:
        parser.error("Can't use -s and -k together, pick one!")
    if options.all and not options.remote and not has_paramiko and (options.kill or options.resubmit):
        parser.error("Can't use job modification options (-s, -k) with -a without paramiko and gssapi.")
    if len(options.xrootd)>0 and options.xrootd[0:7] != "root://" and options.xrootd[0] != "T":
        parser.error("Improper xrootd address: "+options.xrootd)
    if len(options.user)==0:
        parser.error("Must specify a user")
    if len(options.xrootd)>0:
        sitename = ""
        if options.xrootd[0] == "T":
            sitename = options.xrootd
            options.xrootd = parser_dict["manage"]["defaultredir"]
        if options.xrootd[-1] != '/':
            options.xrootd += '/'
        if len(sitename)>0:
            options.xrootd = options.xrootd+"/store/test/xrootd/"+sitename
    if options.ssh or "cmslpc" not in os.uname()[1]: # sometimes "all" shouldn't be used
        options.all = False
    if options.remote:
        options.all = True
    if options.finished>0:
        options.resubmit = False
        options.kill = False
        
    if options.all: all_nodes = parser_dict["schedds"]["fnal"].split(',')
    else: all_nodes = [""]
    for sch in all_nodes:
        jobs = getJobs(options,sch)
        if len(jobs)>0:
            if len(sch)>0: print sch
            if options.resubmit and not options.remote:
                # ssh to local for modification access to scheduler
                client = paramiko.SSHClient()
                # use kerberos authentication
                client.connect(sch,gss_host=sch,gss_auth=True,gss_kex=True)
                # sanitize arguments
                if "-e" in sys.argv:
                    eindex = sys.argv.index("-e")+1
                    sys.argv[eindex] = "'"+sys.argv[eindex]+"'"
                if "-g" in sys.argv:
                    gindex = sys.argv.index("-g")+1
                    sys.argv[gindex] = "'"+sys.argv[gindex]+"'"
                if "-v" in sys.argv:
                    vindex = sys.argv.index("-v")+1
                    sys.argv[vindex] = "'"+sys.argv[vindex]+"'"
                # recursive run
                client.exec_command("cd "+os.getcwd())
                stdin, stdout, stderr = client.exec_command("python "+sys.argv[0]+" --ssh "+' '.join(sys.argv[1:]))
                stdoutlines = stdout.readlines()
                stderrlines = stderr.readlines()
                print ''.join(stdoutlines)
                stderrlinesjoined = ''.join(stderrlines)
                if len(stderrlinesjoined)>1: print stderrlinesjoined
                client.close()
            else:
                printJobs(jobs,options.num,options.progress,options.stdout,options.why,options.matched)

            # resubmit or remove jobs
            if options.resubmit:
                resubmitJobs(jobs,options,sch)
            elif options.kill:
                # get scheduler
                schedd = getSchedd(sch,options.coll)
                # actions that can be applied to all jobs
                jobnums = [j.num for j in jobs]
                schedd.act(htcondor.JobAction.Remove,jobnums)


if __name__=="__main__":
    manageJobs()
