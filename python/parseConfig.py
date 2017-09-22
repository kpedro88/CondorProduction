import os, inspect
from ConfigParser import SafeConfigParser
from collections import defaultdict

def list_callback(option, opt, value, parser):
    if value is None: return
    setattr(parser.values, option.dest, value.split(','))

parser = SafeConfigParser()

# first look in this script's dir, then current dir, then user $HOME (nonexistent files are skipped)
mypath = inspect.getsourcefile(list_callback).replace("parseConfig.py","")
candidates = [os.path.join(mypath,'.prodconfig'), os.path.join(os.getcwd(),'.prodconfig'), os.path.expanduser('~/.prodconfig')]

parser.read(candidates)

# convert to dict

parser_dict = defaultdict(lambda: defaultdict(str),{s:defaultdict(str,parser.items(s)) for s in parser.sections()})
