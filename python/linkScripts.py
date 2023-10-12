from __future__ import print_function
import os
from optparse import OptionParser
from parseConfig import parser_dict

def linkScripts(dir):
    scripts = parser_dict["common"]["scripts"].split(',')
    for script in scripts:
        # automatic & atomic updating of symlinks
        scriptname = os.path.basename(script)
        scriptmp = scriptname+"_tmp"
        scriptpath = os.path.join(dir,script)
        if os.path.isfile(scriptpath):
            os.symlink(scriptpath,scriptmp)
            os.rename(scriptmp,scriptname)
        else:
            print("Cannot locate "+script+" in "+dir)

if __name__=="__main__":
    parser = OptionParser()
    parser.add_option("-d", "--dir", dest="dir",
        default=os.path.expandvars("$CMSSW_BASE/src/Condor/Production"), help="location of top-level CondorProduction directory (default = %default)")
    (options, args) = parser.parse_args()

    linkScripts(options.dir)
