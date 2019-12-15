#Makefile for Project_2019_2

CC=gcc -std=gnu11

FLAGS= -Wall -g
TESTFLAGS= -Wall -g -lcunit

all: queries tests
	./tests

notests: queries

queries_fast: execute_query.c middle_list.c main.c query.c queue.c quicksort.c radix_sort.c relation.c sort_merge_join.c string_list.c table.c
	$(CC) -O3 -o queries execute_query.c middle_list.c main.c query.c queue.c quicksort.c radix_sort.c relation.c sort_merge_join.c string_list.c table.c

tests: tests.o radix_sort.o quicksort.o queue.o relation.o sort_merge_join.o table.o string_list.o execute_query.o middle_list.o query.o
	$(CC) -o tests tests.o radix_sort.o quicksort.o queue.o relation.o sort_merge_join.o table.o string_list.o execute_query.o middle_list.o query.o $(TESTFLAGS)

queries: execute_query.o middle_list.o main.o query.o queue.o quicksort.o radix_sort.o relation.o sort_merge_join.o string_list.o table.o
	$(CC) $(FLAGS) -o queries execute_query.o middle_list.o main.o query.o queue.o quicksort.o radix_sort.o relation.o sort_merge_join.o string_list.o table.o

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

tests.o: tests.c
	$(CC) $(FLAGS) -c tests.c

clean:
	rm -f *.o queries tests
