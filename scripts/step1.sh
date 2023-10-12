#!/bin/bash

echo "Starting job on "`date` # to display the start date
echo "Running on "`uname -a` # to display the machine where the job is running
echo "System release "`cat /etc/redhat-release` # and the system release
echo "CMSSW on Condor"

# check arguments
export CMSSWVER=""
export CMSSWLOC=""
export CMSSWXRD=""
export OPTIND=1
while [[ $OPTIND -le $# ]]; do
	# getopts in silent mode, don't exit on errors
	OPTOLD=$OPTIND
	getopts ":C:L:X:" opt
	case "$opt" in
		C) export CMSSWVER=$OPTARG
		;;
		L) export CMSSWLOC=$OPTARG
		;;
		X) export CMSSWXRD=$OPTARG
		;;
		# keep going if getopts had an error, but make sure not to skip anything
		\? | :) OPTIND=$((OPTOLD+1))
		;;
	esac
done

echo ""
echo "parameter set:"
echo "CMSSWVER: $CMSSWVER"
if [ -n "$CMSSWLOC" ]; then
	echo "CMSSWLOC: $CMSSWLOC"
fi
if [ -n "$CMSSWXRD" ]; then
	echo "CMSSWXRD: $CMSSWXRD"
fi
echo ""

# to get condor-chirp from CMSSW
export PATH="/usr/libexec/condor:$PATH"
# environment setup
source /cvmfs/cms.cern.ch/cmsset_default.sh

# three ways to get CMSSW: tarball transferred by condor, tarball transferred by xrdcp (address provided), new release (SCRAM_ARCH provided)
if [[ "$CMSSWXRD" == root:* ]]; then
	echo "Getting CMSSW via xrdcp"
	xrdcp -f ${CMSSWXRD}/${CMSSWVER}.tar.gz .
	XRDEXIT=$?
	if [[ $XRDEXIT -ne 0 ]]; then
		echo "exit code $XRDEXIT, failure in xrdcp"
		exit $XRDEXIT
	fi
elif [ -n "$CMSSWLOC" ]; then
	echo "Getting CMSSW via cmsrel"
	export SCRAM_ARCH=${CMSSWLOC}
fi

# use a tarball if we have it, otherwise make a new release area
set -e
if [ -e ${CMSSWVER}.tar.gz ]; then
	# workaround
	if [ -n "$CMSSWLOC" ]; then
		mkdir tmp
		tar -xzf ${CMSSWVER}.tar.gz -C tmp
		scram project ${CMSSWVER}
		# omits hidden dirs such as .SCRAM
		cp -r tmp/${CMSSWVER}/* ${CMSSWVER}/
		cd ${CMSSWVER}
	else
		tar -xzf ${CMSSWVER}.tar.gz
		cd ${CMSSWVER}
		scram b ProjectRename
		scram b ExternalLinks
	fi
else
	scram project ${CMSSWVER}
	cd ${CMSSWVER}
fi
# cmsenv
eval `scramv1 runtime -sh`
set +e
