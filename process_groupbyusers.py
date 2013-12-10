#!/usr/bin/env python
#-*- coding=utf8 -*-

author = "Wei Wang"
email = "tskatom@vt.edu"

from multiprocessing import Pool
import sys
from itertools import groupby
import os
from glob import glob
import time


#chunk the file by the user segment(first 3 characters)
def group_by_user(chunk_file, out_dir):
    start = time.time()
    fileHandles = {}
    baseName = os.path.basename(chunk_file)    
    with open(chunk_file) as cf:
        #group the rows by the first 3 characters(users group)
        lines = cf.xreadlines()
        for line in lines:
            key = line[0:3]
            if key not in fileHandles:
                outf = os.path.join(out_dir, "%s_%s" % (key, baseName))
                fileHandles[key] = open(outf, "w")
            fileHandles[key].write(line)
    for key in fileHandles:
        fileHandles[key].flush()
        fileHandles[key].close()
    print "Done: %s in [%s]" % (chunk_file, time.time() - start)                

def main():
    start = time.time()
    infile = sys.argv[1]
    out_dir = sys.argv[2]
    num_process = int(sys.argv[3])

    pool = Pool(processes=num_process)
    files = glob(os.path.join(infile, "*"))
    for f in files:
        pool.apply_async(group_by_user, args=(f, out_dir))

    pool.close()
    pool.join()
    print "All The jobs done in [%s]" % (time.time() - start)

if __name__ == "__main__":
    main()
