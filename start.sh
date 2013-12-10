#parameters define
#experiment id such 4.5
EXP_ID=$1

WORK_FOLDER=/home/tskatom/miami/code/
GRAPH_FILE=/home/tskatom/miami/miami.gph
EXP_DIR=/home/tskatom/miami/experiment4.5/seirexperiment4.5/
MERGE_EXP_FILE=/home/tskatom/miami/processed/ALL_experiments_${EXP_ID}
GRAPH_ADJ_FILE=/home/tskatom/miami/processed/miami_adj.gph
CHUNK_FOLDER=/home/tskatom/miami/processed/chunked/
MERGE_GRAPH_RESULT=/home/tskatom/miami/processed/Experiment_Graph_Merge.result
USER_INFO=/home/tskatom/miami/processed/user_info.json
USER_MERGE_GRAPH_OUTPUT=/home/tskatom/miami/processed/User_Experiment_Graph_Merge.out
FEATURE_OUT_PREFIX=/home/tskatom/miami/processed/Count_Feature
RESULT_FOLDER=/home/tskatom/miami/processed
NUM_PROCESSES=15

#merge and transform the experiment data into one big file
#take around 40 minutes to run
#using multiprocess to transform experiment data with 15 core
#la take 1.5 hour
#boston take 17 min
#dallas take 25 min
python ${WORK_FOLDER}process_transfrom_expe.py ${EXP_DIR} ${OUT_DIR} ${NUM_PROCESSES}

#transform the user adjency
#take around 5 minutes
python ${WORK_FOLDER}transform_graph.py ${GRAPH_FILE} ${GRAPH_ADJ_FILE}

#chunk and push data to DDFS
#take around 4 minuts
split -a 5 -d -l 600000 ${GRAPH_ADJ_FILE} ${CHUNK_FOLDER}EG
split -a 5 -d -l 2000000 ${MERGE_EXP_FILE} ${CHUNK_FOLDER}EX

#run the mapreduce job
#take 18 minutes to run the job
#take 50 minutes to write the data to file(Need to optimize, which is too slow)
python ${WORK_FOLDER}merge_exp_edge_mr.py ${CHUNK_FOLDER} ${MERGE_GRAPH_RESULT}

#add user information into the data
#take around 50minutes to add userinfo(Need to optimize, which is too slow)
python ${WORK_DIR}add_user_info.py ${USER_INFO} ${MERGE_GRAPH_RESULT} ${USER_MERGE_GRAPH_OUTPUT}

#split the result USER_MERGE_GRAPH_OUTPUT file
split -a 5 -d -l 4000000 ${USER_MERGE_GRAPH_OUTPUT} ${CHUNK_FOLDER}UEG

#count the features
#take around 6 minutes to run for each counting
python ${WORK_FOLDER}count_edges_mr.py ${CHUNK_FOLDER} ${FEATURE_OUT_PREFIX} a
python ${WORK_FOLDER}count_edges_mr.py ${CHUNK_FOLDER} ${FEATURE_OUT_PREFIX} aa
python ${WORK_FOLDER}count_edges_mr.py ${CHUNK_FOLDER} ${FEATURE_OUT_PREFIX} ala

#output the top K feature
python ${WORK_FOLDER}data_prepare.py ${FEATURE_OUT_PREFIX} ${RESULT_FOLDER}
