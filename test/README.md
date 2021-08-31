# CondorProduction Tests

This directory provides a simple example of a fully-implemented `jobSubmitter` with two different operations.

The available options for the job submitter are:
* `-N, --nParts [num]`: number of parts to process (default = 1)
* `--name [name]`: job name (default = test)
* `--io`: use I/O options (default = False)
* `--indir [dir]`: input file directory (local or PFN) (default = )
* `--outdir [dir]`: output file directory (local or PFN) (default = )
* `--inpre [str]`: input file prefix (default = )
* `--outpre [str]`: output file prefix (required) (default = )
* `--fail [num]`: fail with specified error code (default = 0)

The default operation (if `--io` is not provided) will just print some information on the worker node.

The secondary, more involved operation is a mockup of a job that takes inputs and produces outputs.
This operation is useful to test the `stageOut` function and to construct and test job chains.
For this operation, the `--outdir` and `--outpre` arguments are required, but the input arguments are not necessarily required.

## Setup

```bash
python $CMSSW_BASE/src/Condor/Production/python/linkScripts.py
ln -s $CMSSW_BASE/src/Condor/Production/python/manageJobs.py .
```

## Example commands

These example commands test the checkpoint/recovery feature of job chains.

Before running these commands, set the shell variable `OUTDIR` to the directory of your choice.

```bash
python submitJobs.py -p -k -t cmsrel --no-queue-arg --intermediate -N 1 --name job0 --io --outdir $OUTDIR --outpre step1
python submitJobs.py -p -k -t cmsrel --no-queue-arg --intermediate -N 1 --name job1 --io --indir ../job0 --inpre step1 --outdir $OUTDIR --outpre step2 --fail 1
python submitJobs.py -p -k -t cmsrel --no-queue-arg -N 1 --name job2 --io --indir ../job1 --inpre step2 --outdir $OUTDIR --outpre step3
python $CMSSW_BASE/src/Condor/Production/python/createChain.py -n chainTest -l job0 -c -j $CMSSW_BASE/src/Condor/Production/test/jobExecCondor_job0.jdl $CMSSW_BASE/src/Condor/Production/test/jobExecCondor_job1.jdl $CMSSW_BASE/src/Condor/Production/test/jobExecCondor_job2.jdl
condor_submit jobExecCondor_chainTest.jdl
```

The job will fail and be held because an artificial failure was requested in job1.

To remove this artificial failure, recover the output from job0, and finish the rest of the job chain, rerun the relevant commands and release the job:
```bash
python submitJobs.py -p -k -t cmsrel --no-queue-arg --intermediate -N 1 --name job1 --io --indir ../job0 --inpre step1 --outdir $OUTDIR --outpre step2
python $CMSSW_BASE/src/Condor/Production/python/createChain.py -n chainTest -l job0 -c -j $CMSSW_BASE/src/Condor/Production/test/jobExecCondor_job0.jdl $CMSSW_BASE/src/Condor/Production/test/jobExecCondor_job1.jdl $CMSSW_BASE/src/Condor/Production/test/jobExecCondor_job2.jdl
python manageJobs.py -hsa
```
