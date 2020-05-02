#!/bin/bash

KEEPTAR=""
XRDIR=""
NOVOMS=""
CMD=""
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

if [[ "${XRDIR}" == *"root://"* ]]; then
	CMD="xrdcp"
elif [[ "${XRDIR}" == *"gsiftp://"* ]]; then
	CMD="env -i X509_USER_PROXY=${X509_USER_PROXY} gfal-copy"
elif [[ -n "${XRDIR}" ]]; then
	echo "ERROR Unknown transfer protocol for the tarball"
	exit 1
fi

if [ -n "$XRDIR" ] && [ -n "$CMD" ]; then
	${CMD} -f ${CMSSW_VERSION}.tar.gz ${XRDIR}/${CMSSW_VERSION}.tar.gz
fi
