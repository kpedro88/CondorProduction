# CondorProduction

A general, extensible set of Python and Bash scripts to submit any job to the [HTCondor](https://research.cs.wisc.edu/htcondor/) batch system.

This package focuses on the use of Condor with [CMSSW](http://cms-sw.github.io/) via the [LPC](https://lpc.fnal.gov/computing/index.shtml)
or [CMS Connect](https://connect.uscms.org/).

Currently in an alpha state of development; caveat emptor.

Table of Contents
=================
* [Installation](#installation)
* [Job submission](#job-submission)
   * [Modes of operation](#modes-of-operation)
      * [Job prototypes](#job-prototypes)
      * [Count mode](#count-mode)
      * [Prepare mode](#prepare-mode)
      * [Submit mode](#submit-mode)
      * [Missing mode](#missing-mode)
      * [Clean mode](#clean-mode)
   * [Job steps](#job-steps)
      * [Step1](#step1)
         * [CMSSW tarball creation](#cmssw-tarball-creation)
      * [Step2 and beyond](#step2-and-beyond)
   * [Summary of options](#summary-of-options)
* [Job management](#job-management)
* [Job chains](#job-chains)
* [Configuration](#configuration)
* [Dependencies](#dependencies)
* [Examples](#examples)

(Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc))

## Installation

This recipe assumes the use of CMSSW. This repository is treated as a CMSSW package,
to take advantage of automatic path setup via scram.
```
cmsrel [CMSSW_VERSION]
cd [CMSSW_VERSION]/src
cmsenv
git clone git@github.com:kpedro88/CondorProduction.git Condor/Production
scram b
$CMSSW_BASE/src/Condor/Production/scripts/postInstall.sh [options]
```

The script [./scripts/postInstall.sh] handles common post-installation actions.
It can be called at the end of the installation script for packages that use CondorProduction.

Its options are:
* `-d [dir]`: location of CondorProduction (default = `$CMSSW_BASE/src/Condor/Production`)
* `-b [dir]`: batch directory for `linkScripts.py`
* `-c`: run `cacheAll.py`
* `-p`: (re)install python3 bindings for htcondor
* `-h`: display help message and exit

The actions it performs are:
1. If `-b`  is provided, go to that directory and link to any scripts (list provided in the `.prodconfig` file (see [Configuration](#configuration)) that will be reused when submitting jobs.
(If the job directory is tracked in git, one can add the symlinked file names to a `.gitignore` file in the directory to exclude them from the repository.)
2. If `-c` is provided, call `cacheAll.py` to mark unneeded directories as excluded from the CMSSW tarball (see [CMSSW tarball creation](##cmssw-tarball-creation)).
3. If `-p` is provided and python3 is available from CMSSW, the correct htcondor bindings for the system Condor will be installed.
The CMSSW [virtualenv](http://cms-sw.github.io/venv.html) will be enabled if not already in use.

## Job submission

Job submission (and several related functions) are handled by a class called [jobSubmitter](./python/jobSubmitter.py)
with an associated driver script [submitJobs.py](./python/submitJobs.py).

The `jobSubmitter` class as provided here is intended to be used as a base class, extended by the user as necessary.
The class is designed with many modular functions that can be overriden by the user.
If the user wishes to add to the base functionality rather than just overriding it, the base class function
can be called using the Python `super()` function.
If the user wishes to change or remove default options, `jobSubmitter` has a member function `removeOptions()`
to simplify the operation of removing options from the option parser.
All defined options are added to `jobSubmitter` as member variables for easier access in its member functions.

Because the logic of job organization is highly variable and task-dependent, the `generateSubmission()` function is left unimplemented.
The user must implement this function to fill the list of [job prototypes](#job-prototypes).
However, a function `generatePerJob()` is provided to perform common operations while creating job prototypes.

### Modes of operation

There are several modes of operation for `jobSubmitter`: count, prepare, submit, missing, and clean.
1. count (`-c, --count`): count the expected number of jobs to submit.
2. prepare (`-p, --prepare`): prepare JDL file(s) and any associated job inputs.
3. submit (`-s, --submit`): submit jobs to Condor.
4. missing (`-m, --missing`): check for missing jobs.
5. clean (`-l, --clean`): clean up log files.

The modes count, submit, and missing are treated as mutually exclusive. The mode prepare can be run with any of them.
(If prepare is not run before submit, the job submission will fail.)

At least one mode must be specified to run the script.

#### Job prototypes

Internally, `jobSubmitter` stores job information in a list `protoJobs`, where each entry is an instance of the class `protoJob`:
* `name`: base name for the set of jobs (see [submit mode](#submit-mode))
* `chainName`: alternate name if jobs are run in a chain (see [job chains](#job-chains) and [missing mode](#missing-mode))
* `nums`: list of job numbers in this set (i.e. `$(Process)` values)
* `njobs`: total number of jobs (used in [count mode](#count-mode))
* `jdl`: JDL filename for this set of jobs
* `queue`: queue command for this set of jobs
* `patterns`: `OrderedDict` of find/replace pairs to create the JDL from template
* `appends`: list of strings to append to the JDL

The `protoJob` class also has a function `makeName(num)` to make an individual job name by combining the job base name and a given job number.
This function is important to match job names with finished or running jobs in [missing mode](#missing-mode), and may also be used for other purposes.
The user can override this function if desired:
```
def makeNameNew(self,num):
    ...
protoJob.makeName = makeNameNew
```

#### Count mode

This mode simply counts the number of jobs expected. This is useful if testing some automatic job splitting algorithm
or other criteria to add or remove jobs from a list, or just trying to plan a large submission of jobs.

#### Prepare mode

This mode creates JDL files from the template file (by default, [jobExecCondor.jdl](./scripts/jobExecCondor.jdl)).
It uses the `patterns` specified in each `protoJob` to perform find-replace operations on the template file
to create a real, usable JDL file. (Currently, regular expressions are not supported.)
It then appends any requested additions (in the `protoJob` `appends`) to the end of the real JDL file.

#### Submit mode

The submit mode calls the `condor_submit` command for each JDL file.
Typically, a user may split a given task into a large number of jobs to run in parallel.
For efficiency, it is best to submit a set of jobs to condor all at once, using the `Queue N` syntax.
For each value 0 &le; n &lt; N, a job will be submitted; Condor internally uses the variable `$(Process)` to store the value of n.
The value of `$(Process)` can be used to differentiate each individual job in the set.
To allow reuse of the same JDL in case of resubmitting just a few removed jobs (see [Missing mode](#missing-mode) below),
the `Queue N` line in the JDL is commented out, and instead the `-queue` option of `condor_submit` is used.
In case this is not desired or possible for some reason (e.g. due to an old version or wrapper of `condor_submit`),
the option `-q, --no-queue-arg` can be used.

#### Missing mode

The missing mode looks at both output files (from finished jobs) and running jobs (in the Condor queue) to determine
if any jobs have been removed.
It has an option to make a resubmission script with a specified name: `-r, --resub [script_name.sh]`.
(Otherwise, it will just print a list of missing jobs.)
It also has an option `-u, --user [username]` to specify which user's jobs to check in the Condor queue.
The default value for `user` can be specified in the `.prodconfig` file (see [Configuration](#configuration)).
The option `-q, --no-queue-arg` can also be used here; in this case, the JDL file will be modified
with the list of jobs to be resubmitted (instead of using `-queue`).
In case [job chains](#job-chains) are used, running jobs may have different names than the output files from finished jobs.
The `protoJob.chainName` attribute is available to convert between the different naming schemes.
To facilitate in-place updates of output files, date ranges can be specified using the `--min-date` and `--max-date` options.

This mode also relies on knowledge of HTCondor collectors and schedulers. Values for the LPC and CMS Connect
are specified in the default `.prodconfig` file (see [Configuration](#configuration)).

#### Clean mode

Similar to missing mode, clean mode looks at output files and running jobs to determine which jobs have finished.
(Finished jobs have output files and are not currently running.)
It puts all the log files from the finished jobs into a compressed tar archive called "log_#.tar.gz", where "#"
is incremented based on the presence of any other log archives in the specified directory.
By default the current directory "." is specified as the output location of the log archive, but this can be 
changed to any local or remote directory with the option `--clean-dir [dir]`.

### Job steps

A typical Condor job consists of several steps:
1. Environment setup
2. Run executable/script
3. Transfer output (stageout)

One focus of this package is standardizing the environment setup step (step1). Accordingly, `jobSubmitter` has a separate set
of member functions focusing on step1. Any subsequent steps should be provided by the user; only a basic amount of setup
is provided in advance. (It is assumed by default that steps 2 and 3 will be combined in a script `step2.sh`, but this
assumption can be changed easily.)

The Condor executable script is [jobExecCondor.sh](./scripts/jobExecCondor.sh), which runs the subroutine scripts
[step1.sh](./scripts/step1.sh), `step2.sh` (user provided), etc. Each subroutine script is sourced and 
the command line arguments are reused (processed by bash `getopts`). Because of this, a special syntax is used with `getopts`
to avoid failing on an unknown option.

The executable script also provides some bash helper functions.
* `getFromClassAd`: parses information from the Condor ClassAds for each job. This can be used, for example, to check the number of
requested CPUs when running a multicore job.
* `stageOut`: copies a local file to a storage element using `xrdcp`. It can retry a specified number of times,
with a specified wait time that increases with each retry. This helps avoid job failure from a temporarily interrupted connection
or unavailable storage element. It can also clean up by removing local files (upon successful or unsuccessful copying).
Staging out can be disabled for intermediate jobs in chains (see [Job chains](#job-chains) below)
by passing the argument `--intermediate` to `jobSubmitter`, which corresponds to the argument `-I` for `jobExecCondor.sh`.

The form of these scripts is tightly coupled with the operations of `jobSubmitter`.
Therefore, by default the scripts are not specified as command-line arguments in Python,
but instead as a member variable in the constructor of `jobSubmitter` (and must be changed explicitly in any
extension of the class by users). The script names are passed to `jobExecCondor.sh` using the `-S` flag.

Jobs can be run in a Singularity container using the [cmssw-env](https://github.com/cms-sw/cmssw-osenv) script.
The name of the container (e.g. `/cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/slc6:latest`) is passed to `jobSubmitter` using the `--env` flag
and to `jobExecCondor.sh` using the `-E` flag.
(Passing other arguments to the script is currently not supported, due to conflicts arising from limitations in Bash `getopts`.)
There are existing CMSSW containers available at `/cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/`;
other Singularity containers hosted on CVMFS can also be used.
Please note, this functionality deliberately does not use the built-in Condor Singularity support,
in order to handle [job chains](#job-chains) in which each job may use a different container (or no container).

#### Step1

The default step1 should be sufficient for most users. It allows a CMSSW environment to be initialized in one of 3 ways:
1. transfer: copy CMSSW tarball from job directory via Condor's `Transfer_Input_Files`.
2. xrdcp: copy CMSSW tarball from specified directory using xrootd.
3. cmsrel: create a new CMSSW area with the specified release and scram architecture.

The Python arguments for the default step1 are:
* `-k, --keep`: keep existing tarball (don't run a `tar` command)
* `-n, --no-voms`: don't check for a VOMS grid proxy (proxy line is removed from JDL template, CMSSW environment via xrdcp not allowed)
* `-t, --cmssw-method [method]`: how to get CMSSW env: transfer, cmsrel, or address for xrdcp (default = transfer)
* `--scram-workaround`: avoid using `scram b ProjectRename` when setting up a CMSSW tarball

The arguments for the default [step1.sh](./scripts/step1.sh) are:
* `-C [CMSSW_X_Y_Z]`: CMSSW release version
* `-L [arg]`: `SCRAM_ARCH` value (if using cmsrel method or workaround)
* `-X [arg]`: CMSSW location (if using xrdcp method)

##### CMSSW tarball creation

The tar command (in [checkVomsTar.sh](./scripts/checkVomsTar.sh)) uses flags `--exclude-vcs` and `--exclude-caches-all` to reduce the size of the CMSSW tarball.
The first flag, `--exclude-vcs`, drops directories like `.git` that may be large but don't contain any useful information for jobs.
The second flag, `--exclude-caches-all`, drops any directory containing a [CACHEDIR.TAG](http://www.brynosaurus.com/cachedir/spec.html) file.

A script [cacheAll.py](./python/cacheAll.py) is provided to expedite the process of using `CACHEDIR.TAG` files.
Directories to cache (or uncache) can be specified in `.prodconfig`. Environment variables used in the directory names will be expanded.

#### Step2 and beyond

As noted, subsequent steps should be implemented and provided by the user.

Some default Python arguments are provided in case the user is using the default JDL [template file](./scripts/jobExecCondor.jdl):
* `--jdl [filename]`: name of JDL template file for job
* `--disk [amount]`: amount of disk space to request for job [kB] (default = 10000000)
* `--memory [amount]`: amount of memory to request for job [MB] (default = 2000)
* `--cpus [number]`: number of CPUs (threads) for job (default = 1)
* `--sites [list]`: comma-separated list of sites for global pool running (if using CMS Connect) (default from `.prodconfig`)
* `--env [args]`: run in Singularity environment using cmssw-env (default = None)

A few other Python arguments are not explicitly included in the default setup, but may often be added by users:
* `-o, --output [dir]`: path to output directory
* `-v, --verbose`: enable verbose output (could be a bool or an int, if different verbosity levels are desired)
By default, missing mode will try to get a list of output files from the `output` option, if it exists.

One shell argument is effectively reserved if the user wants to use the [job management](#job-management) tools:
* `-x [redir]`: xrootd redirector address or site name (for reading input files)

### Summary of options

<details>
<summary>Python</summary>

"Mode of operation" options:
* `-c, --count`: count the expected number of jobs to submit
* `-p, --prepare`: prepare JDL file(s) and any associated job inputs
* `-s, --submit`: submit jobs to Condor
* `-m, --missing`: check for missing jobs
* `--min-date`: minimum date for files in missing mode
* `--max-date`: minimum date for files in missing mode
* `-r, --resub [script_name.sh]`: create resubmission script
* `-l, --clean`: clean up log files
* `--clean-dir`: output dir for log file .tar.gz (default = ".")
* `-u, --user [username]`: view jobs from this user (default from `.prodconfig`)
* `-q, --no-queue-arg`: don't use -queue argument in condor_submit

Default step1 options:
* `-k, --keep`: keep existing tarball (don't run a `tar` command)
* `-n, --no-voms`: don't check for a VOMS grid proxy (proxy line is removed from JDL template, CMSSW environment via xrdpc not allowed)
* `-t, --cmssw-method [method]`: how to get CMSSW env: transfer, cmsrel, or address for xrdcp (default = transfer)
* `--scram-workaround`: avoid using `scram b ProjectRename` when setting up a CMSSW tarball

Default extra options:
* `--jdl [filename]`: name of JDL template file for job
* `--disk [amount]`: amount of disk space to request for job [kB] (default = 1000000)
* `--memory [amount]`: amount of memory to request for job [MB] (default = 2000)
* `--cpus [number]`: number of CPUs (threads) for job (default = 1)
* `--sites [list]`: comma-separated list of sites for global pool running (if using CMS Connect) (default from `.prodconfig`)
* `--env [args]`: args to run job in Singularity environment using cmssw-env (default = None)
* `--intermediate`: specify that this is an intermediate job in a chain to disable staging out
* `--singularity [image]`: specify singularity image for job (default = "")

"Reserved", but not actually used by default:
* `-o, --output [dir]`: path to output directory
* `-v, --verbose`: enable verbose output (could be a bool or an int, if different verbosity levels are desired)
</details>

<details>
<summary>Shell</summary>

"Mode of operation" options:
* `-S [scripts]`: comma-separated list of subroutine scripts to run
* `-E [args]`: args to run job in Singularity environment using cmssw-env
* `-I`: indicate that this job is an intermediate job in a chain (see [Job chains](#job-chains) below); this disables `stageOut` if used in Step2

Default step1 options:
* `-C [CMSSW_X_Y_Z]`: CMSSW release version
* `-L [arg]`: `SCRAM_ARCH` value (if using cmsrel method or workaround)
* `-X [arg]`: CMSSW location (if using xrdcp method)

Default extra options:  
none

"Reserved", but not actually used by default:
* `-x [redir]`: xrootd redirector address or site name (for reading input files)
</details>

## Job management

Jobs submitted with `jobSubmitter` and the default JDL file are set up so that they are held if they exit unsuccessfully
(with a signal or non-zero exit code). Failed jobs therefore stay in the queue and can be released to run again, assuming
the problem is understood. (Problems reading input files over xrootd are common.)

The Python script [manageJobs.py](./python/manageJobs.py) can list information about jobs (held or otherwise) and release them if desired.
It uses a number of command line options to specify how to display job information, modify jobs, and change job statuses:
* `-c, --coll [collector]`: view jobs from this collector (use collector of current machine by default)
* `-u, --user [username]`: view jobs from this user (submitter) (default taken from `.prodconfig`)
* `-a, --all`: view jobs from all schedulers (use scheduler of current machine by default)
* `-h, --held`: view only held jobs
* `-r, --running`: view only running jobs
* `-i, --idle`: view only idle jobs
* `-f, --finished [n]`: view only n finished jobs
* `-t, --stuck`: view only stuck jobs (subset of running)
* `-g, --grep [patterns]`: view jobs with [comma-separated list of strings] in the job name or hold reason
* `-v, --vgrep [patterns]`: view jobs without [comma-separated list of strings] in the job name or hold reason
* `-o, --stdout`: print stdout filenames instead of job names
* `-n, --num`: print job numbers along with names
* `-x, --xrootd [redir]`: edit the xrootd redirector (or site name) of the job input
* `-e, --edit [edit]`: edit the ClassAds of the job (JSON dict format)
* `-s, --resubmit`: resubmit (release) the selected jobs
* `-k, --kill`: remove the selected jobs
* `-d DIR, --dir=DIR`: directory for stdout files (used for backup when resubmitting) (default taken from `.prodconfig`)
* `-w, --why`: show why a job was held
* `-m, --matched`: show site and machine to which the job matched (for CMS Connect)
* `-p, --progress`: show job progress (time and nevents)
* `-X, --xrootd-resubmit`: resubmit the jobs based on where the input files are located
* `-C INPUTFILECLASSAD, --input-file-classad=INPUTFILECLASSAD`: HTCondor ClassAd which contains the input file(s) being used within the job (used in combination with `-X`)
* `-D, --dry-run`: don't actually resubmit any jobs (used in combination with `-X`)
* `-K LOGKEY, --log_key=LOGKEY`: key to use to find the correct line(s) in the log file (used in combination with `-X`)
* `-L LOGPATH, --log_path=LOGPATH`: path to the job logs (used in combination with `-X`, default = `pwd`)
* `-U, --prefer-us-sites`: prefer reading inputs from US sites over others (used in combination with `-X`)
* `-V, --verbose`: be more verbose when printing out the resubmission information for each job (used in combination with `-X`)
* `--add-sites=ADDSITES`: comma-separated list of global pool sites to add
* `--rm-sites=RMSITES`: comma-separated list of global pool sites to remove
* `--stuck-threshold [num]`: threshold in hours to define stuck jobs (default = 12)
* `--ssh`: internal option if script is run recursively over ssh
* `--help`: show help message and exit

The options `-h`, `-i`, `-r`, `-f` are exclusive. The options `-s` and `-k` are also exclusive. The option `-a` is currently only supported
at the LPC (where each interactive node has its own scheduler). The script can ssh to each node and run itself to modify the jobs
on that node (because each scheduler can only be accessed for write operations from its respective node).

The option `-X` will resubmit the jobs (just like `-s`), except that it will tell the job to get its input file from a specific site based on the list of sites where that file
is located and some user preferences. Therefore, the options `-X` and `-s` are exclusive, as are the options `-X` and `-k`. The options `-C`, `-D`, `-K`, `-L`, `-U`, and `-V`
are only applicable when using the option `-X`. There are two ways in which the program can find the appropriate input file(s) for the job:
1. specify an HTCondor ClassAd name, where the ClassAd contains a comma-separated list of files (`-C`).
2. specify the location of a set of log files and a key with which to parse the log files (`-L`/`-K`). It is expected that the log will contain a single line with a comma-separated list of files.

The `-x` and `-X` options are similar. If just the `-x` option is used, all of the resubmitted jobs will try to access the input files from a user-specified location. If just the `-X` option is used, the user doesn't specify a particular location, but a set of loose preferences (see below). If both the `-X` and `-x` options are used, then the jobs will preferentially read their input from the site found automatically, with the redirector or site name given by `-x` as a fallback if no suitable site is found.

When using the `-X` option, a user may indicate a preference for specific sites in two ways:
1. The user can broadly prefer sites located in the United States using the `-U` option
2. The user can specify a list of preferred sites using the attribute `preferredsites` in the `.prodconfig` file (see [Configuration](#configuration))

Limitation: the `-X` option relies upon [dasgoclient](https://github.com/dmwm/dasgoclient) for finding the site information for a given file. It is therefore limited by the accuracy of [DAS](https://cmsweb.cern.ch/das/) and only works for centrally produced/tracked [CMS](https://cms.cern/) input files.

## Job chains

Multiple jobs can be chained together in order to run in series.
(This may be useful, for example, to avoid storing large intermediate output files, in order to reduce disk usage requirements.)
A script [createChain.py](./python/createChain.py) is provided to create these chains from individual jobs.
It produces an aggregated input tarball and corresponding JDL file that can be submitted using `condor_submit`
and executes [jobExecCondorChain.sh](./scripts/jobExecCondorChain.sh).

The aggregrate input tarball contains a directory for each job, named (in order) `job0`, `job1`, etc.
Each job is executed in its own directory, with its own environment.
This regular structure can be used during the preparation of the individual jobs;
for example, `job1` can refer to the expected output file from `job0` using the relative path `../job0/[file]`.

The python script's options are:
* `-h, --help`: show help message and exit
* `-n NAME, --name NAME`: name for chain job (required)
* `-j JDLS [JDLS ...], --jdls JDLS [JDLS ...]`: full paths to JDL files (at least one required)
* `-l LOG, --log LOG`: log name prefix from first job (will be replaced w/ chain job name)
* `-c, --checkpoint`: enable checkpointing (if a job fails, save output files from previous job in chain)

The shell script's options are:
* `-J [jobname]`: name for chain job
* `-N [number]`: number of jobs in chain
* `-P [process]`: process number (used to substitute for `$(Process)` if found in individual job arguments)
* `-C`: enable checkpointing (see above)

Several caveats currently apply:
* The argument `-q, --no-queue-arg` should be used when preparing individual jobs.
* It is recommended to use the `xrdcp` method for transferring CMSSW environment in Step1 when preparing individual jobs.
* The aggregate input tarball must be transferred via Condor, not via xrdcp.

## Configuration

Both job submission and management can be configured by a shared config file called `.prodconfig`.
A [default version](./python/.prodconfig) is provided in this repository. It is parsed by [parseConfig.py](./python/parseConfig.py)
using the Python ConfigParser. The config parsing looks for `.prodconfig` in the following locations, in order (later files supersede earlier ones):
1. `.prodconfig` from this repository
2. `.prodconfig` from current directory (e.g. user's job submission directory)
3. `.prodconfig` from user's home area

Expected categories and values:
* `common`
    * `user = [username]`: typically specified in user's home area
    * `scripts = [script(s)]`: list of scripts to link from this repository to user job directory (comma-separated list)
* `submit`
    * `sites = [sites]`: sites for global pool submission (comma-separated list)
* `manage`:
    * `dir = [dir]`: backup directory for logs from failing jobs
    * `defaultredir = [dir]`: default xrootd redirector (if using site name)
    * `preferredsites = [site(s)]`: list of preferred sites for input file specific job resubmission (comma-separated list, descending order of preference)
* `collectors`:
    * `[name] = [server(s)]`: name and associated collector server(s) (comma-separated list)
* `schedds`:
    * `[name] = [server(s)]`: name and associated schedd server(s) (comma-separated list)
* `caches`:
    * `[dir] = [val]`: directory and cache status (1 = cache, 0 = uncache) (one entry per directory)

The name used for the collector and associated schedd(s) must match.

Limitation: if information for Python scripts used directly from this repository is specified in the `.prodconfig` file in location 2
(user job directory), the associated Python script must be run from location 2 in order to pick up the specified information. Current cases:
* log backup directory for `manageJobs.py`
* list of scripts for `linkScripts.py`

## Dependencies

This repository works with Python 2.7 or 3+ and any modern version of bash (4+).

The missing mode of `jobSubmitter` uses the [Condor python bindings](https://htcondor-python.readthedocs.io/en/latest/htcondor_intro.html)
to check the list of running jobs. It will try very hard to find the Condor python bindings, but if they are not available,
it will simply skip the check of running jobs.

In contrast, `manageJobs` absolutely depends on the Condor python bindings. It will also try very hard to find them,
but if they are not available, it cannot run.

For Python 3 usage, the bindings can be installed by `postInstall.sh` (see [Installation](#installation)).

For more information about global pool sites, see 
[Selecting Sites - CMS Connect Handbook](https://ci-connect.atlassian.net/wiki/spaces/CMS/pages/22609953/Selecting+Sites).

## Examples

A very basic example can be found in the [test](./test) directory.

Other repositories that use this package include:
* [TreeMaker](https://github.com/TreeMaker/TreeMaker#submit-production-to-condor) - ntuple production
* [SVJProduction](https://github.com/cms-svj/SVJProduction#condor-submission) - private signal production
* [SVJtagger](https://github.com/kpedro88/SVJtagger/tree/master/uBDT) - BDT training
* [SimDenoising](https://github.com/kpedro88/SimDenoising#batch-submission) - simulated data generation
* [SonicCMS](https://github.com/fastmachinelearning/SonicCMS/tree/v5.2.0/Brainwave) - ML inference
