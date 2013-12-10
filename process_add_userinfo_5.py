from disco.job import Job
from disco.func import chain_reader
from disco.core import result_iterator
import sys
from glob import glob
import os
import multiprocessing as mp
import time

def transformAge(age):
    age = int(age)
    if age >= 0 and age < 5:
        return "A"
    elif age >= 5 and age < 20:
        return "B"
    elif age >= 20 and age < 64:
        return "C"
    else:
        return "D"


class AddOneInfoJob(Job):
    @staticmethod
    def map(line, params):
        infos = line.strip().split()
        if len(infos) == 4: #user info
            try:
                age = transformAge(infos[1])
            except Exception, e:
                print "Error Occur:[%s]" % e, infos
            else:
                yield infos[0], {"age": age, "gender": infos[2], "income": infos[3], "type":"User"}
        elif len(infos) == 6: #merged graph and experiment info
            yield infos[0], {"type": "Edge", "target": infos[1], "exp_id": infos[2],
                              "day": infos[3], "location": infos[4], "duration": infos[5], 
                              "source": infos[0]}

    @staticmethod
    def reduce(row_iter, params):
        from disco.util import kvgroup
        
        for key, vals in kvgroup(sorted(row_iter)):
            vals = list(vals)
            user = None
            for v1 in vals:
                if v1["type"] == "User":
                    user = v1
                    yield key, v1
                    break
            if user:
                for v1 in vals:
                    if v1["type"] == "Edge":
                        v1["source_attr"] = user
                        yield v1["target"], v1



class AddSecondInfoJob(Job):
    map_reader = staticmethod(chain_reader)
    @staticmethod
    def map(key_value, params):
        yield key_value[0], key_value[1]

    @staticmethod
    def reduce(row_iter, params):
        from disco.util import kvgroup
        for key, vals in kvgroup(sorted(row_iter)):
            vals = list(vals)
            user = None
            
            for v1 in vals:
                if v1["type"] == "User":
                    user = v1
                    break

            if user:
                for v1 in vals:
                    if v1["type"] == "Edge":
                        yield (v1["source_attr"]["age"], v1["source_attr"]["gender"], v1["source_attr"]["income"],
                               user["age"], user["gender"], user["income"],
                               v1["location"], v1["duration"], v1["day"], v1["exp_id"]), ""


def worker(out_f, result):
    print "Start to Write to [%s]" % out_f
    with open(out_file, "w") as of:
        for k, v in result_iterator(result):
            of.write(" ".join(k) + "\n")

if __name__ == "__main__":
    # Jobs cannot belong to __main__ modules.  So, import this very
    # file to access the above classes.
    import process_add_userinfo as chain
    from subprocess import call
    import math

    city = sys.argv[1]
    in_dir = sys.argv[2]
    out_dir = sys.argv[3]
    user_file = sys.argv[4]
    temp_dir = sys.argv[5]
    size = int(sys.argv[6])
    
    if not os.path.exists(temp_dir):
        os.mkdir(temp_dir)

    #get the potential result files
    files = os.listdir(in_dir)
    files.sort()
    command = "split -a 5 -C 10000000 -d %s %s" % (user_file, os.path.join(temp_dir, "%s_USER_" % city))
    call(command, shell=True)
    
    #run the job for each user segment
    start = time.time()
    #split merger files
    full_fs = [os.path.join(in_dir, f) for f in files][100:]
    count = len(full_fs)
    per_group = int(math.ceil(1.0 * count / size))
    group_full_fs = [full_fs[per_group*i:per_group*(i+1)] for i in range(size)]    
    

    #sequencely handle the data 
    for part, fs in enumerate(group_full_fs):
        #split the files
        for f in fs:
            basename = os.path.basename(f)
            print basename
            exp_id = basename.split(".")[0].split("_")[1]
            command = "split -a 5 -C 20000000 %s %s" % (f, os.path.join(temp_dir, "%s_%s_EXP" % (city, exp_id)))
            call(command, shell=True)
    
        #get input files
        inputs = glob(os.path.join(temp_dir, "%s_*" % city))

    
        job1_name = "%s_%d_merge_user_step1" % (city, part)
        job2_name = "%s_%d_merge_user_step2" % (city, part)
        step1 = chain.AddOneInfoJob().run(input=inputs, partitions=600, name=job1_name)
        step2 = chain.AddSecondInfoJob().run(input=step1.wait(show=False), partitions=600, name=job2_name)
        data = result_iterator(step2.wait(show=False))
        key_f = "%s_%d_fea.txt" % (city, part + 1)
        out_file = os.path.join(out_dir, key_f)
        result = step2.wait(show=False)
        p = mp.Process(target=worker, args=(out_file, result))
        p.start()
        #clean the data removing the segment data
        command = "rm -rf %s" % os.path.join(temp_dir, "*EXP*")
        call(command, shell=True)
