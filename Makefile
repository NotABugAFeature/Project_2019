#Makefile for Project_2019_1

CC=gcc -std=gnu11

FLAGS= -Wall -g
TESTFLAGS= -Wall -lcunit

all: queries

tests: query_test test

test: tests.o radix_sort.o quicksort.o queue.o relation.o sort_merge_join.o table.o string_list.o
	$(CC) -o test tests.o radix_sort.o quicksort.o queue.o relation.o sort_merge_join.o table.o string_list.o $(TESTFLAGS)

query_test: query_test.o query.o table.o
	$(CC) $(TESTFLAGS) -o query_test query_test.o query.o table.o


queries: execute_query.o middle_list.o new_main.o query.o queue.o quicksort.o radix_sort.o relation.o sort_merge_join.o string_list.o table.o
	$(CC) $(FLAGS) -o queries execute_query.o middle_list.o new_main.o query.o queue.o quicksort.o radix_sort.o relation.o sort_merge_join.o string_list.o table.o

new_main.o: new_main.c
	$(CC) $(FLAGS) -c new_main.c

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

query_test.o: ./tests/query_test.c
	$(CC) $(FLAGS) -c ./tests/query_test.c

tests.o: ./tests/tests.c
	$(CC) $(FLAGS) -c ./tests/tests.c

clean:
	rm -f *.o queries test query_test
