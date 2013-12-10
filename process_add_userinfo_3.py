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
                uid = infos[0]
                age = transformAge(infos[1])
                gender = infos[2]
                income = infos[3]
            except Exception, e:
                print "Error Occur:[%s]" % e, infos
            else:
                yield uid, {"age": age, "gender": gender, "income": income, "type":"User"}
        elif len(infos) == 6: #merged graph and experiment info
            source_id = infos[0]
            target_id = infos[1]
            exp_id = infos[2]
            day = infos[3]
            loca = infos[4]
            duration = infos[5]
            yield source_id, {"type": "Edge", "target": target_id, "exp_id": exp_id,
                              "day": day, "location": loca, "duration": duration, 
                              "source": source_id}

    @staticmethod
    def reduce(row_iter, params):
        from disco.util import kvgroup
        
        for key, vals in kvgroup(sorted(row_iter)):
            vals = list(vals)
            #get user info
            user = None
            for v1 in vals:
                if v1["type"] == "User":
                    user = v1
                    yield key, v1
            #get edge info
            if user:
                for v2 in vals:
                    if v2["type"] == "Edge":
                        v2["source_attr"] = user
                        yield v2["target"], v2
            

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
            if user:
                for v1 in vals:
                    if v1["type"] == "Edge":
                        source_age = v1["source_attr"]["age"]
                        source_gender = v1["source_attr"]["gender"]
                        source_income = v1["source_attr"]["income"]
                        target_age = user["age"]
                        target_gender = user["gender"]
                        target_income = user["income"]
                        exp_id = v1["exp_id"]
                        day = v1["day"]
                        location = v1["location"]
                        duration = v1["duration"]

                        yield (source_age, source_gender, source_income, 
                               target_age, target_gender, target_income,
                               location, duration, day, exp_id), ""
                        

def write_result(queue, city, user_segs, out_dir):
    #create file handles firstly
    handles = {}
    for user_seg in user_segs:
        key_f = "%s_%s_fea.txt" % (city, user_seg)
        handles[key_f] = open(os.path.join(out_dir, key_f) , "w", 20000)
    while True:
        data = queue.get()
        key_f = data.keys()[0]
        if key_f == "End":
            for key in handles:
                handles[key].flush()
                handles[key].close()
        else:
            file_handle = handles[key_f]
            file_handle.write(" ".join(data[key_f]) + "\n")
        

if __name__ == "__main__":
    # Jobs cannot belong to __main__ modules.  So, import this very
    # file to access the above classes.
    import process_add_userinfo as chain
    from subprocess import call

    city = sys.argv[1]
    in_dir = sys.argv[2]
    out_dir = sys.argv[3]
    user_file = sys.argv[4]
    temp_dir = sys.argv[5]
    
    
    if not os.path.exists(temp_dir):
        os.mkdir(temp_dir)

    #get the potential result files
    files = os.listdir(in_dir)
    
    command = "split -a 5 -C 6000000 -d %s %s" % (user_file, os.path.join(temp_dir, "%s_USER_" % city))
    call(command, shell=True)
    
    #run the job for each user segment
    start = time.time()
    #split merger files
    full_fs = [os.path.join(in_dir, f) for f in files]

    #get input files
    inputs = glob(os.path.join(temp_dir, "%s_*" % city)) + full_fs

    
    job1_name = "%s_merge_user_step1" % (city)
    job2_name = "%s_merge_user_step2" % (city)
    step1 = chain.AddOneInfoJob().run(input=inputs, partitions = 600, name=job1_name)
    step2 = chain.AddSecondInfoJob().run(input=step1.wait(show=False), partitions = 600, name=job2_name)
    data = result_iterator(step2.wait(show=False))
    key_f = "%s_fea.txt" % (city)
    out_file = os.path.join(out_dir, key_f)
    with open(out_file, "w") as of:
        for k, v in result_iterator(step2.wait(show=False)):
            of.write(" ".join(k) + "\n")
    print "Finished %s used [%s]" % (city, time.time() - start)
    #clean the data removing the segment data
    command = "rm -rf %s" % os.path.join(temp_dir, "%s_*" % (city))
    call(command, shell=True)
#    queue.put({"End": True})
