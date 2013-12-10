#!/usr/bin/env python
"""prog usage:
./chunk_n_mr.sh -i edge_file <n1 n2> -n 100 -o out_nodes_file -s get_nodes_mr.py -t True
"""
from disco.core import Job, result_iterator
from disco.func import chain_reader
from disco.worker.classic import modutil
import worker


class CheckEdges(Job):
    params = dict()
    partitions = 600

    def map(self, row, params):
        info = row.strip().split(" ")
        if params["type"] == "a":
            #take infected age as key
            yield(info[3], info[8], info[9]), 1
        elif params["type"] == "aa":
            #take source and infected age as key
            yield(info[0], info[3], info[8], info[9]), 1
        elif params["type"] == "ala":
            #take source, infected and loca as key
            yield (info[0], info[7], info[3], info[8], info[9]), 1

    def reduce(self, rows_iter, out, params):
        from disco.util import kvgroup
        # only those edges are provided to reducer which 
        # which we were trying to locate
        # reducer will output edge (if it exists) and its count
        for edge, vs in kvgroup(sorted(rows_iter)):
            total = sum(vs)             
            out.add(edge, total)
 

if __name__ == '__main__':
    import os
    import sys
    import glob
    from count_edges_mr import CheckEdges
    from disco.ddfs import DDFS
    from disco.error import JobError
    import getpass
    uname = getpass.getuser()
    graph_edges_dir = sys.argv[1]
    out_graph_edge_file = sys.argv[2]
    fea_type = sys.argv[3]
    types = {"a": "Age", "aa": "Age2Age", "ala": "Age_Loc_Age"}
    
    try:
        input_filename = glob.glob(os.path.join(graph_edges_dir, "UEG*"))
        job = CheckEdges().run(
            input=input_filename,
            params = {"type": fea_type},
            name="%s_%s:CountEdges" % (uname, fea_type),
        )
            
        with open(out_graph_edge_file + '_' + types[fea_type], 'w') as out:
            for edge_line, count in result_iterator(job.wait(show=False)):
                out.write("%s %d\n" % (" ".join(edge_line), count))
        print "Job %s Completed!!" % fea_type
    except JobError, jerr:
        print "Job Failed!!!"
        print jerr
    except ValueError, verr:
        print "Job Failed!!!"
        print verr
    except Exception, err:
        print "Job Failed!!!"
        print err
