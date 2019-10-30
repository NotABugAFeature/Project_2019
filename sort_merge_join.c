#include <stdio.h>
#include <stdlib.h>
#include "sort_merge_join.h"

/**
 * Implements sort merge join
 * Takes two relations, sorts them then joins them
 * @param relation *relR The first relation
 * @param relation *relS The second relation
 */
result *sort_merge_join(relation *relR, relation *relS)
{
	printf("R relation:\n");
	print_relation(relR);

	printf("S relation:\n");
	print_relation(relS);

	//Sort the two relations
	radix_sort(relR);
	radix_sort(relS);

	printf("Sorted R:\n");
	print_relation(relR);

	printf("Sorted S:\n");
	print_relation(relS);

	//Join
}