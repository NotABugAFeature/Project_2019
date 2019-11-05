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
int final_join(result_list* list, relation *t, relation *s)
{
    if(list == NULL || t == NULL || s == NULL ||t->tuples==NULL||s->tuples==NULL)
    {
	fprintf(stderr, "%s", "final_join Error: arguments cannot be null\n");
        return 1;
    }    
    for(uint64_t i = 0; i < t->num_tuples; i++)
    {
	uint64_t j = 0;
        while(j < s->num_tuples && t->tuples[i].key >= s->tuples[j].key)
        {
            if(t->tuples[i].key == s->tuples[j].key)
            {
                if(append_to_list(list, t->tuples[i].row_id, s->tuples[j].row_id))
                {
                    fprintf(stderr, "%s", "final_join Error: append to list\n");
                    return 2;
                }
            }
            j++;
        }
    }
    return 0;
}


/**
 * Implements sort merge join
 * Takes two relations, sorts them then joins them
 * @param relation *relR The first relation
 * @param relation *relS The second relation
 * @return Resulting table in result_list format
 */
result_list *sort_merge_join(relation *relR, relation *relS)
{
    if(relR==NULL||relS==NULL||relR->tuples==NULL||relS->tuples==NULL)
    {
        fprintf(stderr, "%s", "sort_merge_join Error: arguments cannot be null\n");
        return NULL;
    }

    printf("R relation:\n");
    print_relation(relR);
    printf("\n");

    printf("S relation:\n");
    print_relation(relS);
    printf("\n");

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

	//printf("\n");
	//printf("Sorted R:\n");
	//print_relation(relR);
	//printf("\n");
    relation_to_file("sorted_relation_R",relR);
	//printf("Sorted S:\n");
	//print_relation(relS);
	//printf("\n");
    relation_to_file("sorted_relation_S",relS);
    result_list *results = create_result_list();
    if(results == NULL)
    {
	fprintf(stderr, "Error in create_result_list\n");
	return NULL;
    }

    //Join
    int res = final_join(results, relR, relS);
    if(res)
    {
        fprintf(stderr, "Error in final_join\n");
        return NULL;
    }

    return results;
}
