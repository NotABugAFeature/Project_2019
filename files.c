#include <stdio.h>
#include <stdlib.h>
#include "histogram.h"

int64_t **read_from_file(char *filename)
{
    FILE *fp;
    int64_t **table;
    uint64_t rows, columns;

    fp = fopen(filename, "rb");
    if(fp == NULL)
    {
        fprintf(stderr, "File not found\n");
        return NULL;
    }

    fscanf(fp, "%llu %llu", &rows, &columns);
    //printf("%llu %llu\n", rows, columns);

    table = malloc(columns * sizeof(int64_t *));
    if(table == NULL)
    {
        fprintf(stderr, "Cannot allocate memory\n");
        return NULL;
    }

    for(uint64_t i = 0; i < columns; i++)
    {
        table[i] = malloc(rows * sizeof(int64_t));
        if(table[i] == NULL)
        {
            fprintf(stderr, "Cannot allocate memory\n");
            return NULL;
        }
    }

    for(uint64_t i = 0; i < rows; i++)
    {
        for(uint64_t j = 0; j < columns; j++)
        {
            fscanf(fp, "%llu", &table[j][i]);
            //printf("%llu\n", table_r[j][i]);
        }
    }

    for(uint64_t i = 0; i < columns; i++)
    {
        for(uint64_t j = 0; j < rows; j++)
        {
            printf("%llu\t", table[i][j]);
        }
        printf("\n");
    }

    return table;
}
