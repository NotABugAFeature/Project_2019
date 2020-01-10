#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include "sort_merge_join.h"

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
int final_join(middle_list* list_t, middle_list* list_s, relation *t, relation *s, uint64_t start_t, uint64_t end_t, uint64_t start_s)
{
    if(list_t==NULL||list_s==NULL||t==NULL||s==NULL||(t->num_tuples>0&&t->tuples==NULL)||(s->num_tuples>0&&s->tuples==NULL)||start_t>t->num_tuples||end_t>t->num_tuples||start_s>s->num_tuples)
    {
	    fprintf(stderr, "%s", "final_join Error: arguments cannot be null\n");
        return 1;
    }
    uint64_t z=start_s;
    for(uint64_t i = start_t; i < end_t; i++)
    {
	    uint64_t j = z;
        while(j < s->num_tuples && t->tuples[i].key >= s->tuples[j].key)
        {
            if(t->tuples[i].key == s->tuples[j].key)
            {
                if(append_to_middle_list(list_t, t->tuples[i].row_id))
                {
                    fprintf(stderr, "%s", "final_join Error: append to list\n");
                    return 2;
                }

		        if(append_to_middle_list(list_s, s->tuples[j].row_id))
                {
                    fprintf(stderr, "%s", "final_join Error: append to list\n");
                    return 2;
                }
            }
	        else
	        {
                z++;
	        }
            j++;
        }
    }
    return 0;
}
