from subprocess import call
import os
import sys

in_dir = "/home/tskatom/miami/processed/chunked/features/miami"
files = os.listdir(in_dir)
out_dir = "/home/tskatom/miami/processed/chunked/features/chunked_miami"

for f in files:
    full_f = os.path.join(in_dir, f)
    out_pred = os.path.join(out_dir, "UEG_%s" % f)
    command = "split -a 5 -d -C 50000000 %s %s" % (full_f, out_pred)
    call(command, shell=True)

