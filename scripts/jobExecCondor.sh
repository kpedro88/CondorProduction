#!/bin/bash

# helper function to structure the ouput into folders
structuredOutput() {
	RES=""
	TMP=${1/.//}
	IFS='_' read -r -a array <<< "$TMP"
	cut=$(expr ${#array[@]} - 2)
	for index in "${!array[@]}"; do
		if [[ $index -lt $cut ]]; then
			if [[ $index -eq 0 ]]; then
				RES=${RES}${array[index]}
			else
				RES=${RES}"_"${array[index]}
			fi
		elif [[ $index -eq $cut ]]; then
			RES=${RES}"/"${array[index]}
		else
			RES=${RES}"_"${array[index]}
		fi
	done
	echo ${RES}
}

# helper function to stage out via xrdcp
stageOut() {
	WAIT=5
	NUMREP=5
	INPUT=""
	OUTPUT=""
	XRDARGS=""
	QUIET=0
	GFAL=0
	CMDSTR="xrdcp"

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
	}

	# set vars used by getopts to local
	local OPTIND OPTARG
	while getopts "i:o:w:n:x:gq" opt; do
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
			gfal-copy $XRDARGS $INPUT $OUTPUT
		else
			xrdcp $XRDARGS $INPUT $OUTPUT
		fi
		XRDEXIT=$?
		if [ $XRDEXIT -eq 0 ]; then
			return 0
		fi
		# in case of bad exit, wait and try again
		TMPWAIT=$(($TMPWAIT + $WAIT))
		if [ $QUIET -eq 0 ]; then echo "Exit code $XRDEXIT, failure in $CMDSTR. Retry after $TMPWAIT seconds..."; fi
		sleep $TMPWAIT
	done

	# if we get here, it really didn't work
	if [ $QUIET -eq 0 ]; then echo "$CMDSTR failed $NUMREP times. It might be an actual problem."; fi
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
export SCRIPTS=""
while [[ $OPTIND -lt $# ]]; do
	# getopts in silent mode, don't exit on errors
	getopts ":S:" opt
	case "$opt" in
		S) export SCRIPTS=$OPTARG
		;;
		# keep going if getopts had an error
		\? | :) OPTIND=$((OPTIND+1))
		;;
	esac
done

IFS="," read -a SCRIPTARRAY <<< "$SCRIPTS"

# execute scripts in order
for SCRIPT in ${SCRIPTARRAY[@]}; do
	cd $_CONDOR_SCRATCH_DIR
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
