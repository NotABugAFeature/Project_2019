#Makefile for Project_2019_1

CC=gcc -std=c11

FLAGS= -Wall
TESTFLAGS= -Wall -lcunit

ifeq ($(IO),true) 

FLAGS=$(CFLAGS) -D_SORTEDTOFILE_

endif

all: join
	@echo "Use (-f) ./join path_to_relR path_to_relS  To Start!"

tests: queue_test quicksort_test radix_sort_test relation_test result_list_test sort_merge_join_test

queue_test: queue_test.o queue.o
	$(CC) $(TESTFLAGS) -o queue_test queue_test.o queue.o

quicksort_test: quicksort_test.o quicksort.o relation.o
	$(CC) $(TESTFLAGS) -o quicksort_test quicksort_test.o quicksort.o relation.o
    
radix_sort_test: radix_sort_test.o radix_sort.o quicksort.o queue.o
	$(CC) $(TESTFLAGS) -o radix_sort_test radix_sort_test.o radix_sort.o quicksort.o queue.o

relation_test: relation_test.o relation.o
	$(CC) $(TESTFLAGS) -o relation_test relation_test.o relation.o
    
result_list_test: result_list_test.o result_list.o
	$(CC) $(TESTFLAGS) -o result_list_test result_list_test.o result_list.o
    
sort_merge_join_test: sort_merge_join_test.o sort_merge_join.o radix_sort.o quicksort.o queue.o result_list.o relation.o
	$(CC) $(TESTFLAGS) -o sort_merge_join_test sort_merge_join_test.o sort_merge_join.o radix_sort.o quicksort.o queue.o result_list.o relation.o

join: main.o sort_merge_join.o radix_sort.o quicksort.o relation.o result_list.o queue.o
	$(CC) $(FLAGS) -o join main.o sort_merge_join.o radix_sort.o quicksort.o relation.o result_list.o queue.o

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

result_list.o: result_list.c result_list.h
	$(CC) $(FLAGS) -c result_list.c

queue.o: queue.c queue.h
	$(CC) $(FLAGS) -c queue.c

queue_test.o: ./tests/queue_test.c
	$(CC) $(FLAGS) -c ./tests/queue_test.c

quicksort_test.o: ./tests/quicksort_test.c 
	$(CC) $(FLAGS) -c ./tests/quicksort_test.c
    
radix_sort_test.o: ./tests/radix_sort_test.c
	$(CC) $(FLAGS) -c ./tests/radix_sort_test.c

relation_test.o: ./tests/relation_test.c
	$(CC) $(FLAGS) -c ./tests/relation_test.c
    
result_list_test.o: ./tests/result_list_test.c
	$(CC) $(FLAGS) -c ./tests/result_list_test.c
    
sort_merge_join_test.o: ./tests/sort_merge_join_test.c
	$(CC) $(FLAGS) -c ./tests/sort_merge_join_test.c

clean:
	rm -f *.o join quicksort_test radix_sort_test relation_test result_list_test sort_merge_join_test
