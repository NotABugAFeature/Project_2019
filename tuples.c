#include "relation.h"
/**
 * Accepts a key column of the table and a relation pointer and creates the
 * rowid, key tuples. The tuples are dynamically allocated so the free function
 * must be called when the relation must be deleted.
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
