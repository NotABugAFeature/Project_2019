#include <stdio.h>
#include <stdlib.h>
#include "histogram.h"

int main(void)
{
    /*int64_t number = 123456789123456789;
    int i = 0;
    for(i=1; i<=8; i++){
        printf("%lld\n", (number >> ((8-i) << 3)) & 0xff);
    }*/

    relation r;
    r.num_tuples = 5;
    r.tuples = malloc(5*sizeof(tuple));

    r.tuples[0].key = 2;
    r.tuples[0].row_id = 1;

    r.tuples[1].key = 1;
    r.tuples[1].row_id = 2;

    r.tuples[2].key = 1;
    r.tuples[2].row_id = 3;

    r.tuples[3].key = 1;
    r.tuples[3].row_id = 4;

    r.tuples[4].key = 4;
    r.tuples[4].row_id = 5;

    uint64_t hist[256] = {0};
    create_histogram(r, 0, 5, hist, 8);

    for(int k=0; k<256; k++)
    {
        if(hist[k] != 0)
        {
            printf("row: %d  key: %llu\n", k, hist[k]);
        }
    }

    transform_to_psum(hist);

    printf("\n\n");
    for(int k=0; k<256; k++)
    {
        printf("row: %d  key: %llu\n", k, hist[k]);
    }

    free(r.tuples);
    return 0;
}
