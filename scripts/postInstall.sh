#!/bin/bash -e

# check for CMSSW environment
if [ -z "$CMSSW_BASE" ]; then
	echo '$CMSSW_BASE not set'
	exit 1
fi

PYTHON=python
if (cd $CMSSW_BASE && scram tool info python3 >& /dev/null); then
	PYTHON=python3
fi

CPDIR=$CMSSW_BASE/src/Condor/Production
BATCHDIR=""
CACHEALL=""
PIPINSTALL=""
usage() {
	ECHO="echo -e"
	$ECHO "postInstall.sh [options]"
	$ECHO
	$ECHO "-d [dir]    \tlocation of CondorProduction (default = $CPDIR)"
	$ECHO "-b [dir]    \tbatch directory for linkScripts.py"
	$ECHO "-c          \trun cacheAll.py"
	$ECHO "-p          \t(re)install python3 bindings for htcondor"
	$ECHO "-h          \tdisplay this help message and exit"
	exit $1
}
# check arguments
while getopts "d:b:cph" opt; do
	case "$opt" in
		d) CPDIR=$OPTARG
		;;
		b) BATCHDIR=$OPTARG
		;;
		c) CACHEALL=true
		;;
		p) PIPINSTALL=true
		;;
		h) usage 0
		;;
	esac
done

if [ -n "$BATCHDIR" ]; then
	(cd $BATCHDIR && $PYTHON $CPDIR/python/linkScripts.py -d $CPDIR)
fi

if [ "$CACHEALL" = true ]; then
	$PYTHON $CPDIR/python/cacheAll.py
fi

if [ "$PIPINSTALL" = true ]; then
	if [ "$PYTHON" != python3 ]; then
		echo "pip install not available for python2"
		exit 1
	fi
	# initial venv setup if needed
	if [ ! -d $CMSSW_BASE/venv ]; then
		scram-venv
		eval `scramv1 runtime -sh`
	fi
	pip3 install --upgrade htcondor==$(condor_version | head -n 1 | cut -d' ' -f2)
fi
