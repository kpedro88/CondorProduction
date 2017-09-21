#!/bin/bash

KEEPTAR=""
XRDIR=""
NOVOMS=""
# check arguments
while getopts "nki:" opt; do
	case "$opt" in
		k) KEEPTAR="keep"
		;;
		i) XRDIR=$OPTARG
		;;
		n) NOVOMS=true
		;;
	esac
done

# grid proxy existence & expiration check
if [ -z "$NOVOMS" ] && ! voms-proxy-info -exists; then
	voms-proxy-init -voms cms --valid 168:00
fi

# tarball of CMSSW area
if [ -z "$KEEPTAR" ]; then
	tar --exclude-caches-all --exclude-vcs -zcf ${CMSSW_VERSION}.tar.gz -C ${CMSSW_BASE}/.. ${CMSSW_VERSION}
fi

if [ -e ${CMSSW_VERSION}.tar.gz ]; then
	ls -lth ${CMSSW_VERSION}.tar.gz
fi

if [ -n "$XRDIR" ]; then
	xrdcp -f ${CMSSW_VERSION}.tar.gz ${XRDIR}/${CMSSW_VERSION}.tar.gz
fi
