#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include "sort_merge_join.h"

int main(int argc, char **argv)
{
	char *file1 = NULL, *file2 = NULL;
	bool result_in_file = false;
	if(argc != 3 && argc != 4)
	{
		fprintf(stderr, "Usage: %s (-f) <table1_file> <table2_file>\n", argv[0]);
		return -1;
	}

	for(int i=0; i<argc; i++)
	{
		if(strcmp(argv[i], "-f") == 0)
		{
			result_in_file = true;
		}
		else
		{
			if(file1 != NULL)
			{
				file2 = argv[i];
				continue;
			}
			file1 = argv[i];
		}
	}

	if(file1 == NULL || file2 == NULL)
	{
		fprintf(stderr, "Usage: %s (-f) <table1_file> <table2_file>\n", argv[0]);
		return -1;
	}
/*
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
*/
	relation *r = relation_from_file(argv[1]);
	if(r == NULL)
	{
		fprintf(stderr, "Error in relation_from_file\n");
		return -2;
	}

	relation *s = relation_from_file(argv[2]);
	if(s == NULL)
	{
		fprintf(stderr, "Error in relation_from_file\n");
		return -2;
	}

    result_list *results = sort_merge_join(r, s);
    if(results == NULL)
    {
    	fprintf(stderr, "Error in sort_merge_join\n");
    	return -3;
    }

    printf("\nResults:\n");
    if(result_in_file)
    {
	FILE *fp = fopen("join_result.txt", "w");
	if(fp == NULL)
	{
	    perror("main: fopen error");
	    return -4;
	}
	print_result_list(results, fp);
	fclose(fp);
    }
    else
    {
	print_result_list(results, stdout);
    }
    printf("\n");

    delete_result_list(results);
    //free(r_table);
    //free(s_table);

    free(r->tuples);
    free(r);

    free(s->tuples);
    free(s);

    return 0;
}
