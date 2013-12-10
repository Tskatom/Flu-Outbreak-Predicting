from disco.core import Job, result_iterator
from multiprocessing import Pool
from glob import glob
import sys
import os
from multiprocessing import Process


class MergeEdge(Job):
    partitions = 300

    def map(self, row, params):
        try:
            key = (row[0], row[1])
            val = {"type": row[2], "val": row[3:]}
        except:
            print row
        else:
            yield key, val

    def reduce(self, row_iter, out, params):
        from disco.util import kvgroup
        from itertools import chain
        for key, vals in kvgroup(sorted(row_iter)):
            vals = list(vals)
            for v1 in vals:
                for v2 in vals:
                    if v1["type"] == "EX" and v2["type"] == "EG":
                        out.add(key, v1["val"] + v2["val"])

    @staticmethod
    def map_reader(fd, size, url, params):
        for row in fd:
            yield row.strip().split()


def runJob(in_dir, out_dir, city, user_seg):
    from merge_exp_edge_mr import MergeEdge

    #get the Graph files
    graph_path = os.path.join(in_dir, "graph/%s/%s_EG*" % (city, user_seg))
    #get the Experiment files
    experiment_path = os.path.join(in_dir, "experiment/%s/%s_EX*" %(city, user_seg))
    
    inputs = glob(graph_path) + glob(experiment_path)
    output = os.path.join(out_dir, "merge/%s/%s_Merge.result" %(city, user_seg))
    job_name = "%s_%s:Merge" % (city, user_seg)

    job = MergeEdge().run(input=inputs, 
                          name=job_name)
    
    result = job.wait(show=False)
    return result, output


def writeTask(output, result):
    with open(output, "w", 8192) as outf:
        for key, val in result_iterator(result):
            outf.write("%s %s\n" % (" ".join(key), " ".join(val)))

def main():
    in_dir = sys.argv[1]
    out_dir = sys.argv[2]
    city = sys.argv[3]
    
    graph_path = os.path.join(in_dir, "graph/%s/" % city)
    files = os.listdir(graph_path)
    user_segs = list(set([f[0:3] for f in files]))

    num_process = len(user_segs)

    for seg in user_segs:
        result, output = runJob(in_dir, out_dir, city, seg)

        p = Process(target=writeTask, args=(output, result))
        p.start()


if __name__ == "__main__":
    main()

