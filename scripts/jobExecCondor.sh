#!/bin/bash

# helper function to stage out via xrdcp
stageOut() {
	if [ $INTERCHAIN -eq 1 ]; then
		# store current path, command, args to rerun later if needed
		if [ -n "$CHECKPOINT_CURR" ]; then
			# execute in subshell to avoid having to cd back
			echo "(" >> ${CHECKPOINT_CURR}
			PATH_TMP=$(realpath --relative-to=${JOBDIR_BASE}/${JOB_CURR} $PWD)
			echo "mkdir -p $PATH_TMP && cd $PATH_TMP" >> ${CHECKPOINT_CURR}
			# preserve quoting in args
			echo "stageOut "$(printf '%q ' "$@") >> ${CHECKPOINT_CURR}
			echo ")" >> ${CHECKPOINT_CURR}
		fi

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
	REVERSE=0

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
		$ECHO "-R            \treverse (stagein): swap input and output"
	}

	# set vars used by getopts to local
	local OPTIND OPTARG
	while getopts "i:o:w:n:x:gqrc:R" opt; do
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
			R) REVERSE=1
			;;
		esac
	done

	if [[ -z "$INPUT" ]] || [[ -z "$OUTPUT" ]]; then
		stageOut_usage
		return 1
	fi

	if [ "$REVERSE" -eq 1 ]; then
		TMPPUT="$INPUT"
		INPUT="$OUTPUT"
		OUTPUT="$TMPPUT"
		# ensure expected output directory exists
		mkdir -p $(dirname $OUTPUT)
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

# needed when using Singularity container for SLC6 (also works for SLC7)
export X509_CERT_DIR=/cvmfs/grid.cern.ch/etc/grid-security/certificates
export X509_VOMSES=/cvmfs/grid.cern.ch/etc/grid-security/vomses
export VOMS_USERCONF=/cvmfs/grid.cern.ch/etc/grid-security/vomses
export X509_VOMSDIR=/cvmfs/grid.cern.ch/etc/grid-security/vomsdir

# check default arguments
TOPDIR=$PWD
export SCRIPTS=""
export USECONT=0
export ARGCONT=""
export INTERCHAIN=0
while [[ $OPTIND -le $# ]]; do
	# getopts in silent mode, don't exit on errors
	OPTOLD=$OPTIND
	getopts ":S:E:I" opt
	case "$opt" in
		S) export SCRIPTS=$OPTARG
		;;
		E) export USECONT=1
		   export ARGCONT="$OPTARG"
		;;
		I) export INTERCHAIN=1
		;;
		# keep going if getopts had an error, but make sure not to skip anything
		\? | :) OPTIND=$((OPTOLD+1))
		;;
	esac
done

# check if need to launch singularity
if [ $USECONT -eq 1 ] && [ -z "$INCONT" ]; then
	export INCONT=1
	# environment setup
	source /cvmfs/cms.cern.ch/cmsset_default.sh
	UNPACKED_IMAGE=$ARGCONT cmssw-env -B $TOPDIR --pwd $TOPDIR -- $0 "$@"
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
		source ${SCRIPT}
		echo ""
	else
		echo "Could not find ${SCRIPT}"
		exit 1
	fi
done
