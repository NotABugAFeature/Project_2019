#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include "result_list.h"
#include "radix_sort.h"

int main(int argc, char **argv)
{
	if(argc != 3)
	{
		fprintf(stderr, "Please give the files with the tables\n");
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
	if(create_relation_from_table(&(rt->array[0][0]), rt->columns, r) != 0)
	{
		fprintf(stderr, "Error creating relation\n");
		return -2;
	}
	/*printf("Creating s relation\n");
	if(create_relation_from_table(&(st->array[0][0]), st->columns, s) != 0)
	{
		fprintf(stderr, "Error creating relation\n");
		return -2;
	}*/

	printf("\nR:\n");
	print_relation(r);

	relation *rr = malloc(sizeof(relation));
	rr->num_tuples = r->num_tuples;
	rr->tuples = malloc(r->num_tuples*sizeof(tuple));

	radix_sort(1, r, rr, 0, r->num_tuples);

	printf("\nSorted R:\n");
	print_relation(rr);

//////////////////////////////////////////////////

	printf("Reading from %s\n", argv[2]);
	table *st = read_from_file(argv[2]);
	
	if(st == NULL)
	{
		fprintf(stderr, "Error reading tables\n");
		return -2;
	}

	printf("ST: \n");
	printf("%" PRIu64 " columns and %" PRIu64 " rows\n", st->columns, st->rows);
	for(int64_t i=0; i<st->rows; i++)
	{
		for(int64_t j=0; j<st->columns; j++)
		{
			printf("%" PRId64 " ", st->array[i][j]);
		}
		printf("\n");
	}

	relation *r1 = malloc(sizeof(relation));
	r1->tuples = NULL; r1->num_tuples = 0;
	//relation *s = malloc(sizeof(relation));
	//s->tuples = NULL; s->num_tuples = 0;

	printf("Creating r1 relation\n");
	if(create_relation_from_table(&(st->array[0][0]), st->columns, r1) != 0)
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
	print_relation(r1);

	relation *rr1 = malloc(sizeof(relation));
	rr1->num_tuples = r1->num_tuples;
	rr1->tuples = malloc(r1->num_tuples*sizeof(tuple));

	radix_sort(1, r1, rr1, 0, r1->num_tuples);

	printf("\nSorted R:\n");
	print_relation(rr1);

//////////////////////////////


	printf("\n\n\nFINAL JOIN\n");
	// print_relation(rr);
	// printf("\n\n\nFINAL JOIN\n");
	// print_relation(rr1);
	result_list *list = create_result_list();
	final_join(list, rr, rr1);

	printf("\n\n\nFINAL RESULTS\n");
	print_result_list(list);

//////////////////


	//printf("\nS:\n");
	//print_relation(s);
	free(r->tuples);
	free(r);
	free(rr->tuples);
	free(rr);

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