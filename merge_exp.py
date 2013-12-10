from subprocess import call
import sys
from multiprocessing import Pool
import time
import os

def worker(exp_id, out_dir):
    print "Start: [%s]" % exp_id
    exp_segs = os.path.join(out_dir, "%s_*" % exp_id)
    out_file = os.path.join(out_dir, "ser_%s.experiment" % exp_id)

    command = "cat %s > %s" %(exp_segs, out_file)
    call(command, shell=True)

    command = "rm -rf %s" % exp_segs
    call(command, shell=True)


if __name__ == "__main__":
    city_dir = sys.argv[1]
    num_process = int(sys.argv[2])
    files = os.listdir(city_dir)
    exps = list(set([f.split("_")[0] for f in files]))
    start = time.time()
    pool = Pool(processes=num_process)
    for exp in exps:
        pool.apply_async(worker, args=(exp, city_dir))

    pool.close()
    pool.join()

    print "ALl Done[%s]" % (time.time() - start)
