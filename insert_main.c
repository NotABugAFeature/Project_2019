#include <stdio.h>
#include <stdlib.h>
#include "sort_merge_join.h"

int main(int argc, char **argv)
{
	if(argc != 2)
	{
		return -1;
	}

	relation *rel = relation_from_file(argv[1]);
	printf("OK\n");
	printf("\n");

	print_relation(rel);

	free(rel->tuples);
	free(rel);

	return 0;
}