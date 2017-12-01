#!/bin/bash

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
