#include <stdio.h>
#include <stdlib.h>
#include "relation.h"

/**
 * Reads a table from a file
 *
 * @param filename - path of the file
 */
int64_t **read_from_file(char *filename)
{
    FILE *fp;
    int64_t **table;
    uint64_t rows, columns;

    //open file
    fp = fopen(filename, "rb");
    if(fp == NULL)
    {
        fprintf(stderr, "File not found\n");
        return NULL;
    }

    fscanf(fp, "%llu %llu", &rows, &columns);

    //allocate memory
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

    //read content
    for(uint64_t i = 0; i < rows; i++)
    {
        for(uint64_t j = 0; j < columns; j++)
        {
            fscanf(fp, "%llu", &table[j][i]);
        }
    }

    /*for(uint64_t i = 0; i < columns; i++)
    {
        for(uint64_t j = 0; j < rows; j++)
        {
            printf("%llu\t", table[i][j]);
        }
        printf("\n");
    }*/

    return table;
}


/**
 * Accepts a key column of the table and a relation pointer and creates the
 * rowid, key tuples. The tuples are dynamically allocated so the free function
 * must be called when the relation must be deleted.
 *
 * @param int64_t* The key column of the table
 * @param uint64_t The number of items in the column
 * @param relation* The relation where the tuples will be stored
 * @return 
 */
int create_relation_from_table(int64_t* key_column,uint64_t column_size,relation* rel)
{
    //Check parameters can be removed
    if(key_column==NULL||rel==NULL||rel->num_tuples!=0||rel->tuples!=NULL)
    {
        fprintf(stderr, "%s", "create_relation_from_table: wrong parameters\n");
        return 1;
    }

    //Allocate memory for the tuples
    rel->tuples=malloc(column_size*sizeof(tuple));
    if(rel->tuples==NULL)
    {
        fprintf(stderr, "%s", "create_relation_from_table: tuple malloc failed\n");
        return 2;
    }

    rel->num_tuples=column_size;

    //Copy the keys and columns ids
    for(uint64_t i=0;i<column_size;i++)
    {
        rel->tuples[i].key=key_column[i];
        rel->tuples[i].row_id=i;
    }
    return 0;
}


/**
 * Prints all the tuples of the relation given
 * 
 * @param rel* The relation to print
 */
void print_relation(relation* rel)
{
    //Check parameters can be removed
    if(rel==NULL||rel->tuples==NULL)
    {
        fprintf(stderr, "%s", "print_relation: NULL parameter\n");
        return;
    }

    printf("Number of tuples:%ld \n",rel->num_tuples);
    printf("RowID\tKey\n");

    for(uint64_t i=0;i<rel->num_tuples;i++)
    {
        printf("%ld\t%ld\n",rel->tuples[i].row_id,rel->tuples[i].key);
    }
}
/**
 * Prints the rowid and keys stored in the tuples
 *
 * @param tuple* Pointer to the array of tuples
 * @param uint64_t The number of items to print (0 - items-1)
 */
void print_tuples(tuple* t,uint64_t items)
{
    //Check parameters //can be removed
    if(t==NULL)
    {
        fprintf(stderr, "%s", "print_tuple: NULL parameter\n");
        return;
    }
    printf("Number of items:%ld \n",items);
    printf("RowID\tKey\n");
    for(uint64_t i=0;i<items;i++)
    {
        printf("%ld\t%ld\n",t[i].row_id,t[i].key);
    }
}
