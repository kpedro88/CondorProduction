#!/bin/bash -e

# check default arguments
export SCRIPTS=""
while [[ $OPTIND -lt $# ]]; do
	# getopts in silent mode, don't exit on errors
	getopts ":S:" opt || status=$?
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
