#!/bin/bash

echo "Starting job on "`date` # to display the start date
echo "Running on "`uname -a` # to display the machine where the job is running
echo "System release "`cat /etc/redhat-release` # and the system release
if [ -n "$SINGULARITY_CONTAINER" ]; then
	echo "Singularity container $SINGULARITY_CONTAINER"
fi
echo ""

export JOBNAME=""
export PART=""
export INDIR=""
export OUTDIR=""
export INDPRE=""
export OUTPRE=""
export FAIL=0
export OPTIND=1
while [[ $OPTIND -le $# ]]; do
	OPTOLD=$OPTIND
	# getopts in silent mode, don't exit on errors
	getopts ":j:p:i:o:n:u:f:" opt || status=$?
	case "$opt" in
		j) export JOBNAME=$OPTARG
		;;
		p) export PART=$OPTARG
		;;
		i) export INDIR=$OPTARG
		;;
		o) export OUTDIR=$OPTARG
		;;
		n) export INPRE=$OPTARG
		;;
		u) export OUTPRE=$OPTARG
		;;
		f) export FAIL=$OPTARG
		;;
		# keep going if getopts had an error, but make sure not to skip anything
		\? | :) OPTIND=$((OPTOLD+1))
		;;
	esac
done

# default mode
if [ -z "$PART" ]; then
	echo "This is a test job. It will print the current directory, contents, and environment."
	echo ""
	echo "Current directory:"
	echo $PWD
	echo ""
	echo "Current directory listing:"
	ls -ltha
	echo ""
	echo "Current environment:"
	printenv
# IO mode
else
	echo "parameter set:"
	echo "JOBNAME: $JOBNAME"
	echo "PART: $PART"
	echo "INDIR: $INDIR"
	echo "OUTDIR: $OUTDIR"
	echo "INPRE: $INPRE"
	echo "OUTPRE: $OUTPRE"

	OUTFNAME=${OUTPRE}_${PART}.log
	# get input if any
	if [ -n "$INDIR" ]; then
		INFNAME=${INPRE}_${PART}.log
		if [[ "$INDIR" == "root://"* ]]; then
			mkdir -p tmp
			xrdcp ${INDIR}/${INFNAME} tmp/
			INDIR=tmp
		fi
		if [ ! -f ${INDIR}/${INFNAME} ]; then
			echo "Missing input: ${INFNAME}"
			exit 1
		fi
		cat ${INDIR}/${INFNAME} >> ${OUTFNAME}
	fi

	if [[ $FAIL -ne 0 ]]; then
		echo "artificial failure $FAIL"
		exit $FAIL
	fi

	# create output
	echo "$JOBNAME $PART" >> ${OUTFNAME}

	# check for gfal case
	CMDSTR="xrdcp"
	GFLAG=""
	if [[ "$OUTDIR" == "gsiftp://"* ]]; then
		CMDSTR="gfal-copy"
		GFLAG="-g"
	fi
	# stageout
	echo "$CMDSTR output for condor"
	for FILE in *.log; do
		echo "${CMDSTR} -f ${FILE} ${OUTDIR}/${FILE}"
		stageOut ${GFLAG} -x "-f" -i ${FILE} -o ${OUTDIR}/${FILE} -r -c '*.log' 2>&1
		XRDEXIT=$?
		if [[ $XRDEXIT -ne 0 ]]; then
			echo "exit code $XRDEXIT, failure in ${CMDSTR}"
			exit $XRDEXIT
		fi
	done
fi
