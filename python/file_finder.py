import os
import subprocess
import sys

def fprint(msg, newline=True):
    import sys
    if newline:
        print msg
    else:
        print msg,
    sys.stdout.flush()

#From: https://stackoverflow.com/questions/1883980/find-the-nth-occurrence-of-substring-in-a-string
def find_nth(haystack, needle, n):
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start+len(needle))
        n -= 1
    return start

def find_site(file_per_job, preferred_sites = None, prefer_us_sites = False, verbose = False):
    file_and_site_per_job = {}
    if verbose:
        fprint("Finding the sites for each file ...", False)
    for i, (job, file) in enumerate(file_per_job.iteritems()):
        if file is None:
            file_and_site_per_job[job] = (file,None,[None])
        else:
            cmd = "dasgoclient -query=\"site file=" + file + "\""
            p = subprocess.Popen(cmd, shell = True, stdout=subprocess.PIPE)
            out, err = p.communicate()
            sites = [None] if "WARNING:" in out else out.split()
            site = select_site(sites, preferred_sites, prefer_us_sites)
            file_and_site_per_job[job] = (file,site,sites)
    if verbose:
        fprint("DONE")
    return file_and_site_per_job

def get_input_file(basepath, jobs, key, verbose = False):
    file_per_job = {}
    if verbose:
        fprint("Finding the input file for each job ...", False)
    for job in jobs:
        output_file = basepath+job.stdout+".stdout"
        if not os.path.exists(output_file):
            file_per_job[job] = None
        else:
            with open(output_file, 'r') as f:
                for line in lines_that_contain(key, f):
                    line = line[line.find("/store/"):line.rfind(".root")+5]
                    line = line.split(",")[0]
                    if "/store/test/xrootd/" in line:
                        line = line[find_nth(line,"/store/",2):]
                    file_per_job[job] = line
    if verbose:
        fprint("DONE")
    return file_per_job

def get_input_file_from_classad(jobs, classad, verbose = False):
    file_per_job = {}
    if verbose:
        fprint("Finding the input file for each job ...", False)
    for job in jobs:
        if hasattr(job, "inputFiles"):
            input_file = job.inputFiles.split(",")[0]
            input_file = input_file[input_file.find("/store/"):input_file.rfind(".root")+5]
            if "/store/test/xrootd/" in input_file:
                input_file = input_file[find_nth(input_file,"/store/",2):]
            file_per_job[job] = input_file
        else:
            file_per_job[job] = None
    if verbose:
        fprint("DONE")
    return file_per_job        

def lines_that_contain(string, fp):
    return [line for line in fp if string in line]

def select_site(sites, preferred_sites = None, prefer_us_sites = False):
    selected = None
    sites = [s.replace("_Disk","") for s in sites if s is not None and "Tape" not in s]
    sites = sorted(sites, key = lambda x: (prefer_us_sites and "US" in x.split('_')[1]), reverse = True)
    if preferred_sites is not None:
        for psite in reversed(preferred_sites):
            if psite in sites:
                sites.insert(0,sites.pop(sites.index(psite)))
    if len(sites) > 0:
        selected = sites[0]
    return selected

def find_input_file_site_per_job(classad = "", condor_jobs = None, log_key = "", log_path = "", preferred_sites = None, prefer_us_sites = False, verbose = False):
    if condor_jobs is None:
        return

    if log_path and log_path[-1] != '/':
        log_path += '/'

    if (log_key and not log_path) or (log_path and not log_key):
        fprint("file_finder.py: error: You must specify both the path to the log files and the key to parse them (--log_key, --log_path).")
        sys.exit(2)

    if classad:
        file_per_job = get_input_file_from_classad(condor_jobs, classad, verbose)
    elif log_path:
        file_per_job = get_input_file(log_path, condor_jobs, log_key, verbose)
    else:
        fprint("file_finder.py: error: You must select a method to obtain the input file information (--classad and/or --log_path/--log_key).")
        sys.exit(2)
    
    file_and_site_per_file = find_site(file_per_job, preferred_sites, prefer_us_sites, verbose)

    return file_and_site_per_file

if __name__ == "__main__":
    find_input_file_site_per_job()