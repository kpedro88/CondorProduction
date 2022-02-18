import argparse
import os
import subprocess
import sys

#From: https://stackoverflow.com/questions/1883980/find-the-nth-occurrence-of-substring-in-a-string
def find_nth(haystack, needle, n):
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start+len(needle))
        n -= 1
    return start

def find_site(file_per_job, prefer_us_sites = False):
    file_and_site_per_job = {}
    print "Finding the sites for each file ...",
    sys.stdout.flush()
    for i, (job, file) in enumerate(file_per_job.iteritems()):
        if file is None:
            file_and_site_per_job[job] = (file,None,[None])
        else:
            cmd = "dasgoclient -query=\"site file=" + file + "\""
            p = subprocess.Popen(cmd, shell = True, stdout=subprocess.PIPE)
            out, err = p.communicate()
            sites = [None] if "WARNING:" in out else out.split()
            site = select_site(sites, prefer_us_sites)
            file_and_site_per_job[job] = (file,site,sites)
    print "DONE"
    return file_and_site_per_job

def get_input_file(basepath, jobs, key):
    file_per_job = {}
    print "Finding the input file for each job ...",
    sys.stdout.flush()
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
    print "DONE"
    return file_per_job

def get_input_file_from_classad(jobs, classad):
    file_per_job = {}
    print "Finding the input file for each job ...",
    sys.stdout.flush()
    for job in jobs:
        if hasattr(job, "inputFiles"):
            input_file = job.inputFiles.split(",")[0]
            input_file = input_file[input_file.find("/store/"):input_file.rfind(".root")+5]
            if "/store/test/xrootd/" in input_file:
                input_file = input_file[find_nth(input_file,"/store/",2):]
            file_per_job[job] = input_file
        else:
            file_per_job[job] = None
    print "DONE"
    return file_per_job        

def lines_that_contain(string, fp):
    return [line for line in fp if string in line]

def select_site(sites, prefer_us_sites = False):
    selected = None
    sites = [s for s in sites if s is not None and "Tape" not in s]
    sites = sorted(sites, key = lambda x: ("FNAL" in x.split('_')[2], "US" in x.split('_')[1] and prefer_us_sites), reverse = True)
    if len(sites) > 0:
        selected = sites[0]
    if selected is not None:
        selected = selected.replace("_Disk","")
    return selected

def find_input_file_site_per_job(argv = None, condor_jobs = None):
    if argv is None:
        argv = sys.argv[1:]
    
    if condor_jobs is None:
        return

    parser = argparse.ArgumentParser(prog='file_finder_resubmitter.py', description = "Resubmit jobs using input from a specific site.")
    parser.add_argument("-c", "--classad", default = "", help = "The HTCondor ClassAd which contains the input file(s) being used within the job (default = %(default)s)")
    parser.add_argument("-k", "--log_key", default = "", help="Key to use to find the correct line(s) in the log file (default = %(default)s)")
    parser.add_argument("-l", "--log_path", default = "", help = "Path of the job logs (default: %(default)s)")
    parser.add_argument("-u", "--prefer_us_sites", action = "store_true", default = False, help = "Preferentially select US sites over others (default: %(default)s)")
    parser.add_argument("--version", action='version', version='%(prog)s v1.0.0')
    args = parser.parse_args(args=argv)

    if args.log_path and args.log_path[-1] != '/':
        args.log_path += '/'

    if (args.log_key and not args.log_path) or (args.log_path and not args.log_key):
        parser.error("You must specify both the path to the log files and the key to parse them (--log_key, --log_path).")

    if args.classad:
        file_per_job = get_input_file_from_classad(condor_jobs, args.classad)
    elif args.log_path:
        file_per_job = get_input_file(args.log_path, condor_jobs, args.log_key)
    else:
        parser.error("You must select a method to obtain the input file information (--classad and/or --log_path/--log_key).")
    
    file_and_site_per_file = find_site(file_per_job, args.prefer_us_sites)

    return file_and_site_per_file

if __name__ == "__main__":
    find_input_file_site_per_job()