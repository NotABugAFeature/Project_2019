#ifndef SORT_MERGE_JOIN_H
#define SORT_MERGE_JOIN_H
//#include "result_list.h"
#include "middle_list.h"
#include "radix_sort.h"

/**
 * Implements the final join of two relations
 * and places the result into a 'result_list'
 *
 * @param list - result list
 * @param t - first relation
 * @param s - second relation
 * @return an integer code {0,1,2}
 */
int final_join(middle_list*, middle_list*, relation *, relation *);


/**
 * Implements sort merge join
 * Takes two relations, sorts them then joins them
 * @param relation *relR The first relation
 * @param relation *relS The second relation
 * @return Resulting table in result_list format
 */ /*
result_list *sort_merge_join(relation *relR, relation *relS); */
#endif	// SORT_MERGE_JOIN_H
