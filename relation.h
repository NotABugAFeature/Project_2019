#ifndef RELATION_H
#define RELATION_H

#include <stdint.h>


/** 
 * Type definition for a table as read from a file
 * rows - the number of initial table's columns 
 * columns - the number of initial table's rows
 * array - an rows x columns array
 */
typedef struct
{
	//uint64_t table_id;
    uint64_t rows;
    uint64_t columns;
    //uint64_t **array;
    uint64_t *data;
}table;


/**
 * Type definition for the index of tables
 * num_tables - the number of tables
 * tables - an array of pointers to tables
 */
typedef struct
{
	uint64_t num_tables;
	table *tables;
}table_index;

/** 
 * Type definition for a tuple
 * key - key of sorting and join operation
 * row_id - incremental id of the tuple
 */
typedef struct
{
    uint64_t key;
    uint64_t row_id;
}tuple;


/** 
 * Type definition for a relation
 * tuples - array of tuples
 * num_tuples - length of tuple array
 */
typedef struct
{
    tuple *tuples;
    uint64_t num_tuples;
}relation;


table *table_from_file(char *);

/**
 * Reads a table from a file
 *
 * @param filename - path of the file
 * @return table in table * format, NULL for error
 */
//table *read_from_file(char *);


/**
 * Frees the momory used by the table
 * @param table*
 */
//void delete_table(table*);


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
//int create_relation_from_table(uint64_t* ,uint64_t , relation*);


/**
 * Reads relation data from file and creates a relation
 * @param char * The name of the file
 * @return relation * A pointer to the created relation
 */
relation *relation_from_file(char *);


/**
 * Stores the relation data from the relation to a file.
 * The file is overwritten
 * @param char * The name of the file
 * @param relation * A pointer to the relation
 * @return int 0 if successful
 */
int relation_to_file(char *,relation*);


/**
 * Prints all the tuples of the relation given
 * 
 * @param rel* The relation to print
 */
void print_relation(relation*);


/**
 * Prints the rowid and keys stored in the tuples
 *
 * @param tuple* Pointer to the array of tuples
 * @param uint64_t The number of items to print (0 - items-1)
 */
void print_tuples(tuple* t,uint64_t items);

#endif	// RELATION_H
