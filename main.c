#include <stdio.h>
#include <stdlib.h>
#include "sort_merge_join.h"

int main(int argc, char **argv)
{
	if(argc != 3)
	{
		fprintf(stderr, "Usage: %s <table1_file> <table2_file>\n", argv[0]);
		return -1;
	}


	table *r_table = read_from_file(argv[1]);
	relation *r = malloc(sizeof(relation));
	if(r == NULL)
	{
		perror("main: malloc error");
		return -2;
	}
	r->tuples = NULL; r->num_tuples = 0;
    create_relation_from_table(&r_table->array[0][0], r_table->columns, r);

    table *s_table = read_from_file(argv[2]);
    relation *s = malloc(sizeof(relation));
    if(s == NULL)
    {
    	perror("main: malloc error");
    	return -2;
    }
    s->tuples = NULL; s->num_tuples = 0;
    create_relation_from_table(&s_table->array[0][0], s_table->columns, s);


    result_list *results = sort_merge_join(r, s);
    if(results == NULL)
    {
    	fprintf(stderr, "Error in sort_merge_join\n");
    	return -3;
    }

    printf("\nResults:\n");
    print_result_list(results);
    printf("\n");

    delete_result_list(results);
    free(r_table);
    free(s_table);

    free(r->tuples);
    free(r);

    free(s->tuples);
    free(s);

    return 0;
}
