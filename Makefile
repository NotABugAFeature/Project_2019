#Makefile for Project_2019_1

CC=gcc -std=c99

FLAGS= -Wall 

all: join
	@echo "Use ./join path_to_relR path_to_relS  To Start!"

join: main.o sort_merge_join.o radix_sort.o quicksort.o relation.o result_list.o
	$(CC) $(FLAGS) -o join main.o sort_merge_join.o radix_sort.o quicksort.o relation.o result_list.o

main.o: main.c
	$(CC) $(FLAGS) -c main.c

sort_merge_join.o: sort_merge_join.c sort_merge_join.h
	$(CC) $(FLAGS) -c sort_merge_join.c

radix_sort.o: radix_sort.c radix_sort.h
	$(CC) $(FLAGS) -c radix_sort.c

quicksort.o: quicksort.c quicksort.h
	$(CC) $(FLAGS) -c quicksort.c

relation.o: relation.c relation.h
	$(CC) $(FLAGS) -c relation.c

result_list.o: result_list.c result_list.h
	$(CC) $(FLAGS) -c result_list.c

clean:
	rm -f *.o join
