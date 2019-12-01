#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include "relation.h"

int main(void)
{
	
	table *t = table_from_file("r0");
	if(t == NULL) return -1;
	printf("Columns: %" PRIu64 ", rows: %" PRIu64 "\n", t->columns, t->rows);
    //printf("First two things: %" PRIu64 " - %" PRIu64 "\n", *(t->data), *(t->data+sizeof(uint64_t)));

    printf("first column:\n");
    for(int i=0; i<t->rows; i++)
    {
    	printf("%" PRIu64 "\n", t->array[0][i]);
    }


    return 0;
}