#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"

import sys
import os


def transform(graph_file, out_file):
    with open(graph_file) as gf, open(out_file, "w") as of:
        #skip the first line
        gf.readline()
        for line in gf.readlines():
            infos = line.strip().split()
            if len(infos) == 2:
                #this is source id
                from_id = infos[0]
            elif len(infos) == 3:
                of.write("%s %s %s %s %s\n" % (from_id, infos[0], "EG", infos[1], infos[2]))

def main():
    graph_file = sys.argv[1]
    out_file = sys.argv[2]
    transform(graph_file, out_file)


if __name__ == "__main__":
    main()

