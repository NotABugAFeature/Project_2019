#ifndef SORT_MERGE_JOIN_H
#define SORT_MERGE_JOIN_H
#include "middle_list.h"
#include "radix_sort.h"

/**
 * Implements the final join of two relations
 * and places the result into two middle lists
 *
 * @param list_t - middle list for first relation
 * @param list_s - middle list for second relation
 * @param t - first relation
 * @param s - second relation
 * @param start_t - staring index of first relation
 * @param end_t - ending index of first relation
 * @param start_s - starting index of second relation
 * @return an integer code {0,1,2}
 */
int final_join(middle_list*, middle_list*, relation *, relation *, uint64_t, uint64_t, uint64_t);

#endif	// SORT_MERGE_JOIN_H
