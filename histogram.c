#include <stdio.h>
#include <stdlib.h>
#include "histogram.h"

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
void create_histogram(relation r, uint64_t start_index, uint64_t end_index, uint64_t *hist, unsigned short byte_number)
{

    for(uint64_t i = start_index; i < end_index; i++)
    {
        uint64_t key = r.tuples[i].key;
        int position = (key >> ((8-byte_number) << 3)) & 0xff;
        hist[position]++;
    }
}

/**
 * Psum creation of a relation r based on its histogram
 *
 * @param hist - histogram
 */
void transform_to_psum(uint64_t *hist)
{
    int first = 1;
    uint64_t offset = 0;

    for(int i = 0; i < HIST_SIZE; i++)
    {
        if(hist[i] != 0)
        {
            if(first)
            {
                offset = hist[i];
                hist[i] = 0;
                first = 0;
            }
            else
            {
                uint64_t temp = offset + hist[i];
                hist[i] = offset;
                offset = temp;
            }
        }
        else
            hist[i] = offset;
    }
}
