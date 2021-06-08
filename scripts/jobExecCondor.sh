#!/bin/bash

# helper function to stage out via xrdcp
stageOut() {
	if [ $INTERCHAIN -eq 1 ]; then
		return 0
	fi

	WAIT=5
	NUMREP=5
	INPUT=""
	OUTPUT=""
	XRDARGS=""
	QUIET=0
	GFAL=0
	CMDSTR="xrdcp"
	REMOVE=0
	CLEANUP=""

	stageOut_usage() {
		case `uname` in
			Linux) ECHO="echo -e" ;;
			*) ECHO="echo" ;;
		esac

		$ECHO "stageOut [options]"
		$ECHO ""
		$ECHO "Options:"
		$ECHO "-i input      \tinput file name (required)"
		$ECHO "-o output     \toutput file name (required)"
		$ECHO "-w wait       \twait time in seconds (default = $WAIT)"
		$ECHO "-n num        \tnumber of repetitions (default = $NUMREP)"
		$ECHO "-x args       \tany arguments to pass to xrdcp/gfal-copy (should be quoted)"
		$ECHO "-g            \tUse gfal-copy rather than xrdcp"
		$ECHO "-q            \tquiet (don't print any messages)"
		$ECHO "-r            \tremove local file if successfully copied"
		$ECHO "-c files      \tcleanup: delete specified file(s) if copy fails"
	}

	# set vars used by getopts to local
	local OPTIND OPTARG
	while getopts "i:o:w:n:x:gqrc:" opt; do
		case "$opt" in
			i) INPUT="$OPTARG"
			;;
			o) OUTPUT="$OPTARG"
			;;
			w) WAIT="$OPTARG"
			;;
			n) NUMREP="$OPTARG"
			;;
			x) XRDARGS="$OPTARG"
			;;
			g) GFAL=1
			   CMDSTR="gfal-copy"
			;;
			q) QUIET=1
			;;
			r) REMOVE=1
			;;
			c) CLEANUP="$OPTARG"
			;;
		esac
	done

	if [[ -z "$INPUT" ]] || [[ -z "$OUTPUT" ]]; then
		stageOut_usage
		return 1
	fi

	# try to copy n times, increasing wait each time
	TMPWAIT=0
	for ((i=0; i < $NUMREP; i++)); do
		if [ $GFAL -eq 1 ]; then
			env -i X509_USER_PROXY=${X509_USER_PROXY} gfal-copy $XRDARGS $INPUT $OUTPUT
		else
			xrdcp $XRDARGS $INPUT $OUTPUT
		fi
		XRDEXIT=$?
		if [ $XRDEXIT -eq 0 ]; then
			if [ $REMOVE -eq 1 ]; then rm $INPUT; fi
			return 0
		fi
		# in case of bad exit, wait and try again
		TMPWAIT=$(($TMPWAIT + $WAIT))
		if [ $QUIET -eq 0 ]; then echo "Exit code $XRDEXIT, failure in $CMDSTR. Retry after $TMPWAIT seconds..."; fi
		sleep $TMPWAIT
	done

	# if we get here, it really didn't work
	if [ $QUIET -eq 0 ]; then echo "$CMDSTR failed $NUMREP times. It might be an actual problem."; fi
	if [ -n "$CLEANUP" ]; then rm $CLEANUP; fi
	return 60000
}

# helper function to parse job classads
# tries to use condor_q if available, if not uses awk
# usage example: VAL=$(getFromClassAd RequestCpus)
getFromClassAd() {
	ARG=$1
	if [ -z "$ARG" ]; then exit 1; fi

	if type condor_q > /dev/null 2>&1; then
		condor_q -jobads ${_CONDOR_SCRATCH_DIR}/.job.ad -af ${ARG}
	else
		ARG=${ARG}" = "
		ARGVAL=$(grep -m 1 "${ARG}" ${_CONDOR_SCRATCH_DIR}/.job.ad)
		if [ -z "$ARGVAL" ]; then exit 1; fi
		awk -v val="${ARGVAL}" -v rem="${ARG}" 'BEGIN { sub(rem,"",val) ; print val }'
	fi
}

# check default arguments
ORIGARGS="$@"
TOPDIR=$PWD
export SCRIPTS=""
export USECONT=0
export ARGCONT=""
while [[ $OPTIND -le $# ]]; do
	# getopts in silent mode, don't exit on errors
	getopts ":S:E:" opt
	case "$opt" in
		S) export SCRIPTS=$OPTARG
		;;
		E) export USECONT=1
		   export ARGCONT="$OPTARG"
		;;
		# keep going if getopts had an error
		\? | :) OPTIND=$((OPTIND+1))
		;;
	esac
done

# check if need to launch singularity
echo "Singularity container: $SINGULARITY_CONTAINER"
if [ $USECONT -eq 1 ] && [ -z "$INCONT" ]; then
	export INCONT=1
	# environment setup
	source /cvmfs/cms.cern.ch/cmsset_default.sh
	cmssw-env $ARGCONT -B $TOPDIR --pwd $TOPDIR -- $0 $ORIGARGS
	exit $?
fi

IFS="," read -a SCRIPTARRAY <<< "$SCRIPTS"

# execute scripts in order
for SCRIPT in ${SCRIPTARRAY[@]}; do
	cd $TOPDIR
	if [ -e ${SCRIPT} ]; then
		echo "Executing ${SCRIPT}"
		echo ""
		# pass command line args in case used
		source $(echo ${SCRIPT})
		echo ""
	else
		echo "Could not find ${SCRIPT}"
		exit 1
	fi
done
