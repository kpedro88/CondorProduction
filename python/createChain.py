import os, shutil, tarfile, glob, six
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from collections import OrderedDict, Callable

class DefaultOrderedDict(OrderedDict):
    # Source: http://stackoverflow.com/a/6190500/562769
    def __init__(self, default_factory=None, *a, **kw):
        if default_factory is not None and not isinstance(default_factory, Callable):
            raise TypeError("first argument must be callable")
        super(DefaultOrderedDict, self).__init__(*a, **kw)
        self.default_factory = default_factory

    def __getitem__(self, key):
        try:
            return super(DefaultOrderedDict, self).__getitem__(key)
        except KeyError:
            return self.__missing__(key)

    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        self[key] = value = self.default_factory()
        return value

    def __reduce__(self):
        if self.default_factory is None:
            args = tuple()
        else:
            args = self.default_factory,
        return type(self), args, None, None, self.items()

    def copy(self):
        return self.__copy__()

    def __copy__(self):
        return type(self)(self.default_factory, self)

    def __deepcopy__(self, memo):
        import copy
        return type(self)(self.default_factory, copy.deepcopy(self.items()))

    def __repr__(self):
        return "OrderedDefaultDict({}, {})".format(self.default_factory, super(DefaultOrderedDict, self).__repr__())

def createChain(jdls,name,log,checkpoint):
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
        if os.path.isdir(subdir_path): shutil.rmtree(subdir_path)
        os.mkdir(subdir_path)
        lines = []
        with open(jdl,'r') as jfile:
            lines = []
            concat_next = False
            for line in jfile:
                line = line.rstrip()
                if len(line)==0: continue
                if concat_next: lines[-1] += line
                else: lines.append(line)
                # detect and handle multi-line
                if line[-1]=="\\":
                    concat_next = True
                    lines[-1] = lines[-1][:-1]
                else:
                    concat_next = False
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
                    file = file.strip()
                    if len(file)==0: continue
                    # todo: find better way to handle "one input file per job" case
                    if "$(Process)" in file:
                        files = glob.glob(file.replace("$(Process)","*"))
                    else:
                        files = [file]
                    for file in files:
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
    final["arguments"] = "-J {} -N {} -P $(Process)".format(name,job_counter)
    final["executable"] = "jobExecCondorChain.sh"
    # checkpoint info is kept using condor file transfer
    if checkpoint:
        checkpoint_dir = "checkpoints_{}".format(name)
        checkpoint_fname1 = "checkpoint_{}_$(Process).txt".format(name)
        checkpoint_fname2 = "{}/{}".format(checkpoint_dir,checkpoint_fname1)
        final["should_transfer_files"] = "YES"
        final["transfer_output_files"] = checkpoint_fname1
        final["transfer_output_remaps"] = '"{} = {}"'.format(checkpoint_fname1,checkpoint_fname2)
        # transfer whole dir to avoid having to make empty checkpoint files
        final[key_transfer] = ','.join([final[key_transfer],checkpoint_dir])
        if not os.path.isdir(checkpoint_dir): os.makedirs(checkpoint_dir)
        final["arguments"] += " -C"
    # write final jdl file
    finalname = "jobExecCondor_{}.jdl".format(name)
    with open(finalname,'w') as ffile:
        ffile.write('\n'.join([key+" = "+val for key,val in six.iteritems(final)])+'\n')
        ffile.write(queue+'\n')

if __name__=="__main__":
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("-n", "--name", dest="name", type=str, required=True, help="name for chain job")
    parser.add_argument("-j", "--jdls", dest="jdls", type=str, default=[], nargs='+', help="full paths to JDL files")
    parser.add_argument("-l", "--log", dest="log", type=str, required=True, help="log name prefix from first job (will be replaced w/ chain job name)")
    parser.add_argument("-c", "--checkpoint", dest="checkpoint", default=False, action="store_true", help="enable checkpointing (if a job fails, save output files from previous job in chain)")
    args = parser.parse_args()
    createChain(args.jdls,args.name,args.log,args.checkpoint)

