#include "result_list.h"
#include "radix_sort.h"

/**
 * Implements the final join of two relations
 * and places the result into a 'result_list'
 * 
 * @param list - result list
 * @param t - first relation
 * @param s - second relation
 */
void final_join(result_list*, relation *, relation *);



/**
 * Implements sort merge join
 * Takes two relations, sorts them then joins them
 * @param relation *relR The first relation
 * @param relation *relS The second relation
 * @return Resulting table in result_list format
 */
result_list *sort_merge_join(relation *relR, relation *relS);
