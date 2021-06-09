#!/bin/bash

JOBNAME=""
NJOBS=0
PROCESS=""
while getopts "J:N:P:" opt; do
	case "$opt" in
		J) JOBNAME=$OPTARG
		;;
		N) NJOBS=$OPTARG
		;;
		P) PROCESS=$OPTARG
		;;
	esac
done

# open aggregate tarball
tar -xzf ${JOBNAME}.tar.gz

# execute each job in series
cd ${JOBNAME}
for ((i=0; i<${NJOBS}; i++)); do
	cd job${i}
	JNAME=$(cat jobname.txt)
	ARGS=$(cat arguments.txt | sed 's/$(Process)/'$PROCESS'/')
	echo "Executing job${i} ($JNAME)"
	./jobExecCondor.sh $ARGS
	JOBEXIT=$?
	if [[ $JOBEXIT -ne 0 ]]; then
		echo "job${i} ($JNAME) failed (exit code $JOBEXIT)"
		exit $JOBEXIT
	fi
	cd ..
done
