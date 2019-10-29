#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include "radix_sort.h"

int main(int argc, char **argv)
{
	if(argc != 2)
	{
		fprintf(stderr, "Please give the file with the table\n");
		return -1;
	}
	printf("Reading from %s\n", argv[1]);
	table *rt = read_from_file(argv[1]);
	//table *st = read_from_file(argv[2]);

	if(rt == NULL)
	{
		fprintf(stderr, "Error reading tables\n");
		return -2;
	}

	printf("RT: \n");
	printf("%" PRIu64 " columns and %" PRIu64 " rows\n", rt->columns, rt->rows);
	for(int64_t i=0; i<rt->rows; i++)
	{
		for(int64_t j=0; j<rt->columns; j++)
		{
			printf("%" PRId64 " ", rt->array[i][j]);
		}
		printf("\n");
	}

	relation *r = malloc(sizeof(relation));
	r->tuples = NULL; r->num_tuples = 0;
	//relation *s = malloc(sizeof(relation));
	//s->tuples = NULL; s->num_tuples = 0;

	printf("Creating r relation\n");
	if(create_relation_from_table(&(rt->array[1][0]), rt->columns, r) != 0)
	{
		fprintf(stderr, "Error creating relation\n");
		return -2;
	}
	/*printf("Creating s relation\n");
	if(create_relation_from_table(&(st->array[1][0]), st->columns, s) != 0)
	{
		fprintf(stderr, "Error creating relation\n");
		return -2;
	}*/

	printf("\nR:\n");
	print_relation(r);
	//printf("\nS:\n");
	//print_relation(s);
	free(r->tuples);
	free(r);
	//free(s->tuples);
	//free(s);
	for(int64_t i=0; i<rt->rows; i++)
	{
		free(rt->array[i]);
	}
	free(rt->array);
	free(rt);
	//free(st->array);
	//free(st);

	return 0;
}