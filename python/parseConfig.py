import os, sys
from ConfigParser import SafeConfigParser
from collections import defaultdict

def list_callback(option, opt, value, parser):
    if value is None: return
    setattr(parser.values, option.dest, value.split(','))

parser = SafeConfigParser()

# first look in current dir, then look in user $HOME (nonexistent files are skipped)
candidates = [os.path.join(sys.path[0],'.prodconfig'), os.path.expanduser('~/.prodconfig')]

parser.read(candidates)

# convert to dict

parser_dict = defaultdict(lambda: defaultdict(str),{s:defaultdict(str,parser.items(s)) for s in parser.sections()})

# special case
if "input" in parser_dict["submit"].keys():
    parser_dict["submit"]["input"] = parser_dict["submit"]["input"].split(',')