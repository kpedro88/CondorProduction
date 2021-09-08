#!/bin/bash

JOBNAME=""
NJOBS=0
PROCESS=""
CHECKPOINT=""
while getopts "J:N:P:C" opt; do
	case "$opt" in
		J) JOBNAME=$OPTARG
		;;
		N) NJOBS=$OPTARG
		;;
		P) PROCESS=$OPTARG
		;;
		C) CHECKPOINT=1
		;;
	esac
done
FIRST_STEP=0

# open aggregate tarball
tar -xzf ${JOBNAME}.tar.gz

# for checkpoints
TOPDIR=$PWD
CHECKPOINT_PRE=${TOPDIR}/checkpoints_${JOBNAME}
mkdir -p ${CHECKPOINT_PRE}
CHECKPOINT_FILE=checkpoint_${JOBNAME}_${PROCESS}
CHECKPOINT_TXT=${CHECKPOINT_PRE}/${CHECKPOINT_FILE}.txt
CHECKPOINT_IN=${CHECKPOINT_PRE}/${CHECKPOINT_FILE}.sh
CHECKPOINT_OUT=${TOPDIR}/${CHECKPOINT_FILE}.txt
CHECKPOINT_BAK=${TOPDIR}/${CHECKPOINT_FILE}.bak
export CHECKPOINT_PREV=""
export CHECKPOINT_CURR=""

# check previous checkpoint
if [ -f "$CHECKPOINT_TXT" ]; then
	# default output: keep previous checkpoint
	cp ${CHECKPOINT_TXT} ${CHECKPOINT_OUT}
	cp ${CHECKPOINT_TXT} ${CHECKPOINT_IN}
	CHECKPOINT_STEP=$(head -n 1 ${CHECKPOINT_IN} | cut -d' ' -f3)
	if [ -n "$CHECKPOINT_STEP" ] && [ -n "$CHECKPOINT" ]; then
		FIRST_STEP=${CHECKPOINT_STEP}
		# set up stagein commands (reverse of stageout)
		sed -i 's/stageOut/stageOut -R/g' ${CHECKPOINT_IN}
	fi
# make sure output file exists (before running any jobs: avoid condor error message about missing output file)
else
	touch ${CHECKPOINT_OUT}
fi

# execute each job in series
export JOBDIR_BASE=${TOPDIR}/${JOBNAME}
export JOB_PREV=""
export JOB_CURR=""
cd ${JOBNAME}
for ((i=${FIRST_STEP}; i<${NJOBS}; i++)); do
	# backup previous checkpoint
	export CHECKPOINT_PREV=${CHECKPOINT_CURR}
	export JOB_PREV=${JOB_CURR}
	export JOB_CURR=job${i}

	cd ${JOB_CURR}
	JNAME=$(cat jobname.txt)
	ARGS=$(cat arguments.txt | sed 's/$(Process)/'$PROCESS'/')

	# recover input files from checkpointed step
	if [[ "$i" == "$CHECKPOINT_STEP" ]]; then
		# CHECKPOINT_CURR not set here, so next step will have blank CHECKPOINT_PREV
		# -> existing CHECKPOINT_OUT will be kept if next step fails again
		echo "Recovering output from ${JOB_CURR} ($JNAME)"
		# need to get env from step1.sh?
		source ${CHECKPOINT_IN}
		CHECKEXIT=$?
		if [[ $CHECKEXIT -ne 0 ]]; then
			echo "exit code $CHECKEXIT, failure recovering output from ${JOB_CURR}"
			exit $CHECKEXIT
		fi
	else
		# advance to next checkpoint
		# only if actually running job (not if recovering)
		export CHECKPOINT_CURR=${CHECKPOINT_PRE}/${JOB_CURR}.sh
		# recovery will execute in job directory
		echo "source jobExecCondor.sh" >> ${CHECKPOINT_CURR}

		echo "Executing ${JOB_CURR} ($JNAME)"
		./jobExecCondor.sh $ARGS
		JOBEXIT=$?
		if [[ $JOBEXIT -ne 0 ]]; then
			# if first job failed, nothing to checkpoint
			if [ -n "$CHECKPOINT" ] && [ -n "$CHECKPOINT_PREV" ]; then
				# checkpoint: stageout files from previous step
				echo "Making checkpoint for ${JOB_PREV}"
				# in subshell just to be safe
				(
				cd ${JOBDIR_BASE}/${JOB_PREV}
				source ${CHECKPOINT_PREV}
				)
				CHEXIT=$?
				if [[ $CHEXIT -ne 0 ]]; then
					# keep previous checkpoint, if any, in this case
					echo "Failed to make checkpoint (exit code $CHEXIT)"
				else
					# transfer back most recent checkpoint via condor
					mv ${CHECKPOINT_PREV} ${CHECKPOINT_OUT}
					# keep track of checkpointed job step number
					sed -i '1s/^/# step '$((i-1))'\n/' ${CHECKPOINT_OUT}
				fi
			fi

			echo "${JOB_CURR} ($JNAME) failed (exit code $JOBEXIT)"
			exit $JOBEXIT
		fi
	fi
	cd ..
done
