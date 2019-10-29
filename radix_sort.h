#ifndef RADIX_SORT_H
#define RADIX_SORT_H
#include "quicksort.h"
//----Histogram----
#define HIST_SIZE 256
/**
 * Histogram creation of a relation r
 * We consider the indexes as valid and we perform no check
 *
 * @param relation r - the table for which we create the histogram
 * @param start_index - the starting index of the relation
 * @param end_index - the ending index is the ending_index - 1
 * @param hist - array already allocated and initialized with 0s
 * @param byte_number - ranges from 1 (most significant left-most byte) to 8 (less significant right-most byte)
 */
void create_histogram(relation r, uint64_t start_index, uint64_t end_index, uint64_t *hist, unsigned short byte_number);
/**
 * Psum creation of a relation r based on its histogram
 *
 * @param hist - histogram
 */
void transform_to_psum(uint64_t *hist);
//----Radix Sort----
/**
 * Copies a part of the source relation to the target relation with the use of
 * the cumulative histogram (psum)
 * @param relation* The source relation
 * @param relation* The target relation
 * @param uint64_t The starting index of the relation
 * @param uint64_t The ending index of the relation (it is not included in
 *                 the transfer)
 * @param uint64_t* The cumulative histogram (psum)
 * @param unsigned Which byte is used to search in the cumulative histogram
 * @return
 */
int copy_relation_with_psum(relation* source, relation* target, uint64_t index_start, uint64_t index_end, uint64_t* psum, unsigned short nbyte);
/**
 * Implements radix sort (uses quicksort, copy_array, create_histogram_from_relation, transform_histogram_to_cumulative_histogram, split_and_fill_array)
 * @param unsigned short byte Which byte is used to create the histogram
 * @param relation *initial The initial array with the data
 * @param relation *result The array to write the results
 * @param uint64_t start_index The starting index of the relation
 * @param uint64_t end_index The ending index of the relation
 */
void radix_sort(unsigned short byte, relation *initial, relation *result, uint64_t start_index, uint64_t end_index);
#endif // RADIX_SORT_H
