#include <stdio.h>
#include "./result_list/result_list.h"
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
	r->tuples = NULL; r->num_tuples = 0;
    create_relation_from_table(&r_table->array[0][0], r_table->columns, r);

    table *s_table = read_from_file(argv[2]);
    relation *s = malloc(sizeof(relation));
    s->tuples = NULL; s->num_tuples = 0;
    create_relation_from_table(&s_table->array[0][0], s_table->columns, s);

    result *res = sort_merge_join(r, s);
    return 0;
}
