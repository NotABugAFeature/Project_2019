#Makefile for Project_2019_2

CC=gcc -std=gnu11

FLAGS= -Wall -g
TESTFLAGS= -Wall -g -lcunit

ifeq ($(sorted_projections),true) 
FLAGS+= -DSORTED_PROJECTIONS
endif

ifeq ($(s_execution),true) 
FLAGS+= -DSERIAL_EXECUTION
endif

ifeq ($(s_join),true) 
FLAGS+= -DSERIAL_JOIN
endif

ifeq ($(s_presorting),true) 
FLAGS+= -DSERIAL_PRESORTING
endif

ifeq ($(s_sorting),true) 
FLAGS+= -DSERIAL_SORTING
endif

ifeq ($(s_filter),true) 
FLAGS+= -DSERIAL_FILTER
endif

ifeq ($(s_selfjoin),true) 
FLAGS+= -DSERIAL_SELFJOIN
endif

ifeq ($(s_projections),true) 
FLAGS+= -DSERIAL_PROJECTIONS
endif

ifeq ($(one_query),true) 
FLAGS+= -DONE_QUERY_AT_A_TIME
endif

ifeq ($(max_queries),true) 
FLAGS+= -DMAX_QUERIES_LIMIT
endif



all: queries tests
	./tests

notests: queries

queries_fast: execute_query.c middle_list.c main.c query.c queue.c quicksort.c radix_sort.c relation.c sort_merge_join.c string_list.c table.c job_fifo.c job_scheduler.c projection_list.c list_array.c
	$(CC) -O3 $(FLAGS) -o queries execute_query.c middle_list.c main.c query.c queue.c quicksort.c radix_sort.c relation.c sort_merge_join.c string_list.c table.c job_fifo.c job_scheduler.c projection_list.c list_array.c -lpthread
	
tests: tests.o radix_sort.o quicksort.o queue.o relation.o sort_merge_join.o table.o string_list.o execute_query.o middle_list.o query.o job_fifo.o job_scheduler.o list_array.o projection_list.o
	$(CC) -o tests tests.o radix_sort.o quicksort.o queue.o relation.o sort_merge_join.o table.o string_list.o execute_query.o middle_list.o query.o job_fifo.o job_scheduler.o list_array.o projection_list.o $(TESTFLAGS)

queries: execute_query.o job_fifo.o job_scheduler.o list_array.o middle_list.o projection_list.o main.o query.o queue.o quicksort.o radix_sort.o relation.o sort_merge_join.o string_list.o table.o
	$(CC) $(FLAGS) -o queries execute_query.o job_fifo.o job_scheduler.o list_array.o middle_list.o projection_list.o main.o query.o queue.o quicksort.o radix_sort.o relation.o sort_merge_join.o string_list.o table.o -lpthread

main.o: main.c
	$(CC) $(FLAGS) -c main.c

sort_merge_join.o: sort_merge_join.c sort_merge_join.h
	$(CC) $(FLAGS) -c sort_merge_join.c

radix_sort.o: radix_sort.c radix_sort.h quicksort.h queue.h relation.h
	$(CC) $(FLAGS) -c radix_sort.c

quicksort.o: quicksort.c quicksort.h relation.h
	$(CC) $(FLAGS) -c quicksort.c

relation.o: relation.c relation.h
	$(CC) $(FLAGS) -c relation.c

queue.o: queue.c queue.h
	$(CC) $(FLAGS) -c queue.c

string_list.o: string_list.c string_list.h
	$(CC) $(FLAGS) -c string_list.c

execute_query.o: execute_query.c execute_query.h
	$(CC) $(FLAGS) -c execute_query.c

middle_list.o: middle_list.c middle_list.h
	$(CC) $(FLAGS) -c middle_list.c

table.o: table.c table.h
	$(CC) $(FLAGS) -c table.c

query.o: query.c query.h
	$(CC) $(FLAGS) -c query.c

job_fifo.o: job_fifo.c
	$(CC) $(FLAGS) -c job_fifo.c

job_scheduler.o: job_scheduler.c
	$(CC) $(FLAGS) -c job_scheduler.c

list_array.o: list_array.c
	$(CC) $(FLAGS) -c list_array.c

projection_list.o: projection_list.c
	$(CC) $(FLAGS) -c projection_list.c

tests.o: tests.c
	$(CC) $(FLAGS) -c tests.c

clean:
	rm -f *.o queries tests
