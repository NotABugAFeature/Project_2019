#Makefile for Project_2019_1

CC=gcc -std=c11

FLAGS= -Wall -g
TESTFLAGS= -Wall -lcunit

ifeq ($(IO),true) 

FLAGS+= -D_SORTEDTOFILE_

endif

all: join
	@echo "Use (-f) ./join path_to_relR path_to_relS  To Start!"

test: tests.o radix_sort.o quicksort.o queue.o relation.o sort_merge_join.o result_list.o table.o string_list.o
	$(CC) -o test tests.o radix_sort.o quicksort.o queue.o relation.o sort_merge_join.o result_list.o table.o string_list.o $(TESTFLAGS)


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

table.o: table.c table.h
	$(CC) $(FLAGS) -c table.c

string_list.o: string_list.c string_list.h
	$(CC) $(FLAGS) -c string_list.c

tests.o: ./tests/tests.c
	$(CC) $(FLAGS) -c ./tests/tests.c

clean:
	rm -f *.o join test
