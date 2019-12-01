#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <fcntl.h>
#include <unistd.h>
#include "relation.h"


/**
 * Accepts a key column of the table and a relation pointer and creates the
 * rowid, key tuples. The tuples are dynamically allocated so the free function
 * must be called when the relation must be deleted.
 *
 * @param uint64_t* The key column of the table
 * @param uint64_t The number of items in the column
 * @param relation* The relation where the tuples will be stored
 * @return 0 for success, >0 for error
 */
int create_relation_from_table(uint64_t* key_column,uint64_t column_size,relation* rel)
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
    	perror("create_relation_from_table: malloc error");
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
 * Reads relation data from file and creates a relation
 * @param char * The name of the file
 * @return relation * A pointer to the created relation
 */
relation *relation_from_file(char *filename)
{
	FILE *fp;

    //open file
    fp = fopen(filename, "rb");
    if(fp == NULL)
    {
        perror("relation_from_file: fopen error");
        return NULL;
    }

    uint64_t rows = 0;
    char str[100];

    while(fgets(str, 99, fp) != NULL)
    {
    	rows++;
    }

    printf("File %s has %" PRIu64 " rows\n", filename, rows);
    relation *rel = malloc(sizeof(relation));
    if(rel == NULL)
    {
    	perror("relation_from_file: malloc error");
	fclose(fp);
    	return NULL;
    }
    rel->num_tuples = rows;
    if(rows == 0)
    {
	fprintf(stderr, "relation_from_file: can't create empty relation\n");
	free(rel);
	fclose(fp);
	return NULL;
    }

    rel->tuples = malloc(rows*sizeof(tuple));
    if(rel->tuples == NULL)
    {
    	perror("relation_from_file: malloc error");
	free(rel);
        fclose(fp);
    	return NULL;
    }

    rewind(fp);
    for(uint64_t i=0; i<rows; i++)
    {
    	fgets(str, 99, fp);
    	if(sscanf(str, "%" PRIu64 ",%" PRIu64, &(rel->tuples[i].key), &(rel->tuples[i].row_id)) != 2)
	{
		fprintf(stderr, "relation_from_file: incorrect file format\n");
		free(rel->tuples);
		free(rel);
		fclose(fp);
		return NULL;
	}
    }

    fclose(fp);
    return rel;
}


/**
 * Stores the relation data from the relation to a file.
 * The file is overwritten
 * @param char * The name of the file
 * @param relation * A pointer to the relation
 * @return int 0 if successful
 */
int relation_to_file(char *filename,relation*rel)
{
    if(filename==NULL||rel==NULL||rel->tuples==NULL||rel->num_tuples==0)
    {
        fprintf(stderr, "%s", "relation_to_file: wrong parameters\n");
        return 1;
    }
    FILE *fp;
    //open file
    fp=fopen(filename, "w");
    if(fp==NULL)
    {
        perror("relation_to_file: fopen error");
        return 2;
    }
    char line[80];
    for(uint64_t i=0; i<rel->num_tuples; i++)
    {
        snprintf(line,80,"%" PRIu64 ",%" PRIu64"\n", rel->tuples[i].key, rel->tuples[i].row_id);
        fprintf(fp,"%s",line);
    }
    fclose(fp);
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

    printf("Number of tuples:%"PRIu64 "\n",rel->num_tuples);
    printf("RowID\tKey\n");

    for(uint64_t i=0;i<rel->num_tuples;i++)
    {
        printf("%" PRIu64 "\t%" PRIu64 "\n",rel->tuples[i].row_id,rel->tuples[i].key);
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
    printf("Number of items:%" PRIu64 " \n",items);
    printf("RowID\tKey\n");
    for(uint64_t i=0;i<items;i++)
    {
        printf("%" PRIu64 "\t%" PRIu64 "\n",t[i].row_id,t[i].key);
    }
}
