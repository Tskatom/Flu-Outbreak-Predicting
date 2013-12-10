import json
import numpy as np

def read_ala(line):
    info = line.split()
    key = "%s_%s_%s" % (info[0], info[1], info[2])
    exp = info[4]
    day = info[3]
    count = info[5]
    return exp, key, day, count


def read_aa(line):
    info = line.split()
    key = "%s_%s" % (info[0], info[1])
    exp = info[3]
    day = info[2]
    count = info[4]
    return exp, key, day, count


def read_a(line):
    info = line.split()
    key = info[0]
    exp = info[2]
    day = info[1]
    count = info[3]
    return exp, key, day, count

TYPE_MATCH = {"ala": read_ala, "aa": read_aa, "a": read_a}

def read(line, t):
    f = TYPE_MATCH[t]
    return f(line)
   
def process(result_file, t):
    data = {}
    with open(result_file) as rf:
        for line in rf:
            exp, key, day, count = read(line.strip(), t)
            if exp not in data:
                data[exp] = {}
            if key not in data[exp]:
                data[exp][key] = {"days":[], "counts":[]}
            data[exp][key]["days"].append(day)
            data[exp][key]["counts"].append(int(count))
    return data   


def daily_infect(days, counts):
    full_counts = np.zeros(300, int)
    for i, d in enumerate(days):
        full_counts[int(d)-1] = counts[i]
    #compute the daily infected and accum
    cumsum_fc = full_counts.cumsum() / (1.0 * sum(full_counts))
    return full_counts, cumsum_fc
            

def compute_top(result, k=3):
    top_k_result = {}
    #get the top k frequent feature in each experiment
    for exp in result:
        top_k_result[exp] = {}
        f_counts = [(fea, sum(result[exp][fea]["counts"])) for fea in result[exp]]
        sorted(f_counts, key=lambda x:x[1], reverse=True)
        top_k = [item[0] for item in f_counts[0:k]]
        #compute the accuminative and daily infected 
        for fea in top_k:
            days = result[exp][fea]["days"]
            counts = result[exp][fea]["counts"]
            daily, d_cumsum = daily_infect(days, counts) 
            top_k_result[exp][fea] = {"daily": list(daily), "cumsum": list(d_cumsum)}
    return top_k_result
    

def main():
    import sys
    import os
    file_name = {"a": "Only_Age.json", "aa": "Age_2_Age.json", "ala": "Age_Lo_Age.json"}
    types = {"a": "Age", "aa": "Age2Age", "ala": "Age_Loc_Age"}
    feature_prefix = sys.argv[1]
    out_folder = sys.argv[2]
    for t in types:
        result_f = feature_prefix + "_" + types[t]
        data = process(result_f, t)
        top_k_result = compute_top(data)
        json.dump(top_k_result, open(os.path.join(out_folder, file_name[t]), "w"))
    

if __name__ == "__main__":
    main()            
