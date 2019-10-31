#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include "relation.h"
#include "result_list.h"

void final_join(result_list* list, relation *t, relation *s)
{
    printf("num_tuples: %" PRIu64 " %" PRIu64 "\n", t->num_tuples, s->num_tuples);
    for(uint64_t i = 0; i < t->num_tuples; i++)
    {
        printf("i: %" PRIu64 "\n", i);
        uint64_t j = 0;
        while(t->tuples[i].key >= s->tuples[j].key && j < s->num_tuples)
        {
            printf("NOW: %" PRIu64 " %" PRIu64 "\n", t->tuples[i].key, s->tuples[j].key);
            if(t->tuples[i].key == s->tuples[j].key)
            {
                printf("FOUND\n");
                if(append_to_list(list, t->tuples[i].key, s->tuples[j].key))
                {
                    perror("Error: append to list");
                    return;
                }
            }
            j++;
        }
    }
}