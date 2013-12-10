from glob import glob
import sys

files = glob("/home/tskatom/miami/processed/chunked/*")
i = 0
for f in files:
    i = i + 1
    print i 
    with open(f) as of:
        j = 0
        for l in of:
            j = j + 1
            le = len(l.split(" "))
            if le != 5:
                print f, j
                sys.exit(0)
