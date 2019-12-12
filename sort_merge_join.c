#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include "sort_merge_join.h"

/**
 * Implements the final join of two relations
 * and places the result into a 'result_list'
 *
 * @param list - result list
 * @param t - first relation
 * @param s - second relation
 * @return an integer code {0,1,2}
 */
int final_join(middle_list* list_t, middle_list* list_s, relation *t, relation *s)
{
    if(list_t==NULL||list_s==NULL||t==NULL||s==NULL||(t->num_tuples>0&&t->tuples==NULL)||(s->num_tuples>0&&s->tuples==NULL))
    {
	fprintf(stderr, "%s", "final_join Error: arguments cannot be null\n");
        return 1;
    }
    uint64_t z=0;
    for(uint64_t i = 0; i < t->num_tuples; i++)
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
