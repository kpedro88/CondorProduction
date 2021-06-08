import os, shutil, tarfile
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from collections import OrderedDict, defaultdict

class OrderedDefaultDict(OrderedDict, defaultdict):
    def __init__(self, default_factory=None, *args, **kwargs):
        #in python3 you can omit the args to super
        super(OrderedDefaultDict, self).__init__(*args, **kwargs)
        self.default_factory = default_factory

def createChain(jdls,name,log):
    final = DefaultOrderedDict(str)
    queue = ""
    if not os.path.isdir(name): os.mkdir(name)
    key_transfer = "transfer_input_files"
    job_counter = 0
    for jdl in jdls:
        jname = os.path.basename(jdl).replace(".jdl","")
        jdir = os.path.dirname(jdl)
        subdir = "job{}".format(job_counter)
        subdir_path = os.path.join(name,subdir)
        if os.path.isdir(subdir): shutil.rmtree(subdir_path)
        os.path.mkdir(subdir_path)
        lines = []
        with open(jdl,'r') as jfile:
            lines = []
            concat_next = False
            for line in jfile:
                if concat_next: lines[-1] += line
                else: lines.append(line)
                # detect and handle multi-line
                if line[-1]=="\\":
                    concat_next = True
                    lines[-1] = lines[-1][:-1]
        for line in lines:
            linesplit = line.split(" = ", 1)
            try:
                key,val = linesplit
            except:
                key = ""
                val = linesplit[0]
            key = key.lower()
            # handle different cases
            # get maximum resource requests
            if key.startswith("request"):
                final[key] = str(max(int(val),int(final[key] if final[key] else 0)))
            # copy input files to subdir
            elif key==key_transfer:
                for file in val.split(','):
                    shutil.copy2(os.path.join(jdir,file),subdir_path)
            # keep each job's arguments separate
            elif key=="arguments":
                argfile = os.path.join(subdir_path,"arguments.txt")
                with open(argfile, 'w') as afile:
                    afile.write(val)
            # generalize log file names
            elif key in ["output","error","log"]:
                if not final[key]: final[key] = val.replace(log, name)
            # omit comments
            elif key.startswith("#"):
                continue
            # handle Queue statements (no-queue-arg preferred), omit duplicates
            elif val.lower().startswith("queue "):
                if not queue: queue = val
            # keep all other arguments, omitting duplicates
            else:
                if not final[key]: final[key] = val
        jnamefile = os.path.join(subdir_path,"jobname.txt")
        with open(jnamefile, 'w') as jnfile:
            jnfile.write(jname)
        job_counter += 1
    # make combined tarball of all job input files
    tarname = "{}.tar.gz".format(name)
    with tarfile.open(tarname,"w:gz") as tar:
        tar.add(name,name)
    # finish up arguments
    final[key_transfer] = "jobExecCondorChain.sh,"+tarname
    final["arguments"] = "-J {} -N {}".format(name,job_counter)
    # write final jdl file
    finalname = "jobExecCondor_{}.jdl".format(name)
    with open(finalname,'w') as ffile:
        ffile.write('\n'.join([key+" = "+val for key,val in final.iteritems()]))
        ffile.write(queue+'\n')

if __name__=="__main__":
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("-n", "--name", dest="name", type=str, required=True, help="name for chain job")
    parser.add_argument("-j", "--jdls", dest="jdls", type=str, default=[], nargs='+', help="full paths to JDL files")
    parser.add_argument("-l", "--log", dest="log", type=str, required=True, help="log name prefix from first job (will be replaced w/ chain job name)")
    args = parser.parse_args()
    createChain(args.jdls,args.name,args.log)
    