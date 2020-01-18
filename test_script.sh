#!/bin/bash
mkdir results
make sorted_projections=true queries_fast;
cat /tmp/temp_medium_dataset/medium.txt | ./queries 1  > ./results/full_par_sp_1_1
cat /tmp/temp_medium_dataset/medium.txt | ./queries 1  > ./results/full_par_sp_1_2
cat /tmp/temp_medium_dataset/medium.txt | ./queries 1  > ./results/full_par_sp_1_3
cat /tmp/temp_medium_dataset/medium.txt | ./queries 2  > ./results/full_par_sp_2_1
cat /tmp/temp_medium_dataset/medium.txt | ./queries 2  > ./results/full_par_sp_2_2
cat /tmp/temp_medium_dataset/medium.txt | ./queries 2  > ./results/full_par_sp_2_3
cat /tmp/temp_medium_dataset/medium.txt | ./queries 4  > ./results/full_par_sp_4_1
cat /tmp/temp_medium_dataset/medium.txt | ./queries 4  > ./results/full_par_sp_4_2
cat /tmp/temp_medium_dataset/medium.txt | ./queries 4  > ./results/full_par_sp_4_3
cat /tmp/temp_medium_dataset/medium.txt | ./queries 8  > ./results/full_par_sp_8_1
cat /tmp/temp_medium_dataset/medium.txt | ./queries 8  > ./results/full_par_sp_8_2
cat /tmp/temp_medium_dataset/medium.txt | ./queries 8  > ./results/full_par_sp_8_3
make sorted_projections=true s_execution=true queries_fast;
cat /tmp/temp_medium_dataset/medium.txt | ./queries 1  > ./results/s_exec_sp_1_1
cat /tmp/temp_medium_dataset/medium.txt | ./queries 1  > ./results/s_exec_sp_1_2
cat /tmp/temp_medium_dataset/medium.txt | ./queries 1  > ./results/s_exec_sp_1_3
cat /tmp/temp_medium_dataset/medium.txt | ./queries 2  > ./results/s_exec_sp_2_1
cat /tmp/temp_medium_dataset/medium.txt | ./queries 2  > ./results/s_exec_sp_2_2
cat /tmp/temp_medium_dataset/medium.txt | ./queries 2  > ./results/s_exec_sp_2_3
cat /tmp/temp_medium_dataset/medium.txt | ./queries 4  > ./results/s_exec_sp_4_1
cat /tmp/temp_medium_dataset/medium.txt | ./queries 4  > ./results/s_exec_sp_4_2
cat /tmp/temp_medium_dataset/medium.txt | ./queries 4  > ./results/s_exec_sp_4_3
cat /tmp/temp_medium_dataset/medium.txt | ./queries 8  > ./results/s_exec_sp_8_1
cat /tmp/temp_medium_dataset/medium.txt | ./queries 8  > ./results/s_exec_sp_8_2
cat /tmp/temp_medium_dataset/medium.txt | ./queries 8  > ./results/s_exec_sp_8_3
make sorted_projections=true one_query=true queries_fast;
cat /tmp/temp_medium_dataset/medium.txt | ./queries 1  > ./results/one_query_sp_1_1
cat /tmp/temp_medium_dataset/medium.txt | ./queries 1  > ./results/one_query_sp_1_2
cat /tmp/temp_medium_dataset/medium.txt | ./queries 1  > ./results/one_query_sp_1_3
cat /tmp/temp_medium_dataset/medium.txt | ./queries 2  > ./results/one_query_sp_2_1
cat /tmp/temp_medium_dataset/medium.txt | ./queries 2  > ./results/one_query_sp_2_2
cat /tmp/temp_medium_dataset/medium.txt | ./queries 2  > ./results/one_query_sp_2_3
cat /tmp/temp_medium_dataset/medium.txt | ./queries 4  > ./results/one_query_sp_4_1
cat /tmp/temp_medium_dataset/medium.txt | ./queries 4  > ./results/one_query_sp_4_2
cat /tmp/temp_medium_dataset/medium.txt | ./queries 4  > ./results/one_query_sp_4_3
cat /tmp/temp_medium_dataset/medium.txt | ./queries 8  > ./results/one_query_sp_8_1
cat /tmp/temp_medium_dataset/medium.txt | ./queries 8  > ./results/one_query_sp_8_2
cat /tmp/temp_medium_dataset/medium.txt | ./queries 8  > ./results/one_query_sp_8_3
make sorted_projections=true s_presorting=true s_sorting=true queries_fast;
cat /tmp/temp_medium_dataset/medium.txt | ./queries 1  > ./results/s_sort_sp_1_1
cat /tmp/temp_medium_dataset/medium.txt | ./queries 1  > ./results/s_sort_sp_1_2
cat /tmp/temp_medium_dataset/medium.txt | ./queries 1  > ./results/s_sort_sp_1_3
cat /tmp/temp_medium_dataset/medium.txt | ./queries 2  > ./results/s_sort_sp_2_1
cat /tmp/temp_medium_dataset/medium.txt | ./queries 2  > ./results/s_sort_sp_2_2
cat /tmp/temp_medium_dataset/medium.txt | ./queries 2  > ./results/s_sort_sp_2_3
cat /tmp/temp_medium_dataset/medium.txt | ./queries 4  > ./results/s_sort_sp_4_1
cat /tmp/temp_medium_dataset/medium.txt | ./queries 4  > ./results/s_sort_sp_4_2
cat /tmp/temp_medium_dataset/medium.txt | ./queries 4  > ./results/s_sort_sp_4_3
cat /tmp/temp_medium_dataset/medium.txt | ./queries 8  > ./results/s_sort_sp_8_1
cat /tmp/temp_medium_dataset/medium.txt | ./queries 8  > ./results/s_sort_sp_8_2
cat /tmp/temp_medium_dataset/medium.txt | ./queries 8  > ./results/s_sort_sp_8_3
