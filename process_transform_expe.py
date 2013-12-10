from multiprocessing import Pool
import os
import glob
import sys
import time
  
def transform(out_dir, exp_file):
    try:
        base_name = os.path.basename(exp_file)
        out_file = os.path.join(out_dir, "EX_%s" % base_name)
        wf = open(out_file, "w")
        with open(exp_file) as ef:
            for line in ef:
                info = line.strip().split(" ", 3)
                if len(info) < 4:
                    continue
                target_id = info[0]
                exid = info[1]
                day = info[2]
                source_ids = info[3].split(" ")
                for s_id in source_ids:
                    wf.write("%s %s %s %s %s\n" % (s_id, 
                                                   target_id,
                                                   "EX", 
                                                   exid, day))
        wf.flush()
        wf.close()
        print "Work Done: %s" % exp_file
    except Exception, e:
        print "Failed To transform %s: [%s]" % (exp_file, e)


def main():
    exp_dir = sys.argv[1] #experiment folder
    out_dir = sys.argv[2] #output folder
    num_process = int(sys.argv[3]) #number of process

    start = time.time()
    
    pool = Pool(processes=num_process)
    experiments = glob.glob(os.path.join(exp_dir, "*.experiment"))
    for expe in experiments:
        pool.apply_async(transform, args=(out_dir, expe))
    pool.close()
    pool.join()
    end = time.time()
    print "Well Done, elpased time: %s " % (end - start)

if __name__ == "__main__":
    main()
