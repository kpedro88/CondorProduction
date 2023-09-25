from __future__ import print_function
import os,six
from parseConfig import parser_dict

def cacheOne(dir,filepath):
    if os.path.isfile(filepath):
        print("Already cached "+dir)
    else:
        print("Cache "+dir)
        with open(filepath,'w') as cachefile:
            cachefile.write('Signature: 8a477f597d28d172789f06886806bc55\n')
            cachefile.write('# This file is a cache directory tag.\n')
            cachefile.write('# For information about cache directory tags, see:\n')
            cachefile.write('#       http://www.brynosaurus.com/cachedir/')

def uncacheOne(dir,filepath):
    if os.path.isfile(filepath):
        print("Uncache "+dir)
        os.remove(filepath)
    else:
        print("Already uncached "+dir)

def cacheAll():
    for dir,val in six.iteritems(parser_dict['caches']):
        filepath = os.path.join(os.path.expandvars(dir),'CACHEDIR.TAG')
        if val=='1':
            cacheOne(dir,filepath)
        elif val=='0':
            uncacheOne(dir,filepath)

if __name__=="__main__":
    cacheAll()
