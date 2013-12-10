#!/bin/env/python
#-*-coding:utf8-*-

author = "Wei Wang"
email = "tskatom@vt.edu"


from multiprocessing import Pool
import os
from glob import glob
import sys
import time


def worker(chunk_file, city, out_dir):
    print "Start: [%s]" % chunk_file
    start = time.time()
    file_handles = {}
    basename = os.path.basename(chunk_file)
    user_seg = basename.split("_")[0]
    with open(chunk_file) as cf:
        for line in cf:
            try:
                exp_id = line.split(" ")[2]
                out_file = os.path.join(out_dir, "%s/%s_%s.experiment" % (city, exp_id, user_seg))
                
                if out_file not in file_handles:
                    file_handles[out_file] = open(out_file, "w", 20000)
                file_handles[out_file].write(line)
            except Exception, e:
                print "Error [%s]" % e
                raise

    for fh in file_handles:
        file_handles[fh].flush()
        file_handles[fh].close()

    print "Done:[%s] Elpsed:[%s]" % (chunk_file, time.time() - start)

if __name__ == "__main__":
    city = sys.argv[1]
    in_dir = sys.argv[2]
    out_dir = sys.argv[3]
    files = glob(os.path.join(in_dir, "*"))
    num_process = len(files)
    pool = Pool(processes=num_process)
    for f in files:
        pool.apply_async(worker, args=(f, city, out_dir))

    pool.close()
    pool.join()

    #clean the data
    for f in files:
        command = "rm -rf %s" % f
        call(command, shell=True)
