#include <stdio.h>
#include <stdlib.h>
#include "sort_merge_join.h"

/**
 * Implements sort merge join
 * Takes two relations, sorts them then joins them
 * @param relation *relR The first relation
 * @param relation *relS The second relation
 * @return Resulting table in result_list format
 */
result_list *sort_merge_join(relation *relR, relation *relS)
{
	printf("R relation:\n");
	print_relation(relR);

	printf("S relation:\n");
	print_relation(relS);

	//Sort the two relations
	int retval = radix_sort(relR);
	if(retval != 0)
	{
		fprintf(stderr, "Error in radix_sort\n");
		return NULL;
	}

	retval = radix_sort(relS);
	if(retval != 0)
	{
		fprintf(stderr, "Error in radix_sort\n");
		return NULL;
	}

	printf("Sorted R:\n");
	print_relation(relR);

	printf("Sorted S:\n");
	print_relation(relS);

	result_list *results = create_result_list();
	if(results == NULL)
	{
		fprintf(stderr, "Error in create_result_list\n");
		return NULL;
	}

	//Join
	final_join(results, relR, relS);

	return results;
}