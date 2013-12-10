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
    user_segs = [f.split(".")[0].split("_")[1] for f in files]
    
#    queue = mp.Queue()
#    p = mp.Process(target=write_result, args=(queue, city, user_segs, out_dir))
#    p.daemon = True
#    p.start()

    #split user file
    command = "split -a 5 -C 6000000 -d %s %s" % (user_file, os.path.join(temp_dir, "%s_USER_" % city))
    call(command, shell=True)
    files = [files[0:10], files[10:20], files[20:30], files[30:40],files[40:50], files[50:60]]
    #run the job for each user segment
    for fs in files:
        start = time.time()
        for f in fs:
            user_seg = f.split(".")[0].split("_")[1]
            #split merger files
            full_f = os.path.join(in_dir, f)
            command = "split -a 5 -C 6000000 -d %s %s" % (full_f, os.path.join(temp_dir, "%s_%s_MERGE_" % (city, user_seg)))
            call(command, shell=True)
        end1 = time.time()
        print "split file cost:[%s]" % (end1 - start)

        #get input files
        inputs = glob(os.path.join(temp_dir, "%s_*" % city))

        #inputs = ['/home/tskatom/test.merge', '/home/tskatom/test.user']
    
        job1_name = "%s_%s_merge_user_step1" % (city, user_seg)
        job2_name = "%s_%s_merge_user_step2" % (city, user_seg)
        step1 = chain.AddOneInfoJob().run(input=inputs, partitions=300, 
                                          name=job1_name)
        end2 = time.time()
        print "job1 cost[%s]" % (end2 - end1)

        step2 = chain.AddSecondInfoJob().run(input=step1.wait(show=False), 
                                             partitions=300, 
                                             name=job2_name)
        end3 = time.time()
        print "job2 cost[%s]" % (end3 - end2)
        key_f = "%s_%s_fea.txt" % (city, user_seg)
        key_f = "%s_%s_fea.txt" % (city, user_seg)
        out_file = os.path.join(out_dir, key_f)
        with open(out_file, "w") as of:
            for k, v in result_iterator(step2.wait(show=False)):
                of.write(" ".join(k) + "\n")
#                queue.put({key_f: k})
        end4 = time.time()
        print "Write to file cost[%s]" % (end4 - end3)
        #todo temp
        #clean the data removing the segment data
        command = "rm -rf %s" % os.path.join(temp_dir, "%s_%s_MERGE_*" % (city, user_seg))
        call(command, shell=True)
        end5 = time.time()
        print "clear used [%s]" % (end5 - end4)
        
        print "Finished %s used [%s]" % (user_seg, time.time() - start)
#    queue.put({"End": True})
