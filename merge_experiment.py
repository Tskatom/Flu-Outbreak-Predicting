#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"

import sys
from glob import glob

experi_dir = sys.argv[1]
out_file = sys.argv[2]

assert experi_dir, "Please enter the experiment Directory"
assert out_file, "Please enter the output file"


files = glob("%s/*.experiment" % experi_dir)
wf = open(out_file, "w")
for f in files:
    print f
    with open(f) as ef:
        for line in ef:
            info = line.strip().split(" ", 3)
            if len(info) < 4:
                continue
            target_id = info[0]
            exid = info[1]
            day = info[2]
            source_ids = info[3].split(" ")
            for s_id in source_ids:
                wf.write("%s %s %s %s %s\n" % (s_id, target_id,
                                               "EX", exid, day))
    wf.flush()

wf.flush()
wf.close()
