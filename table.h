#ifndef TABLE_H
#define TABLE_H

#include "string_list.h"

/**
 * Reads filenames of tables from stdin and returns them in a list
 * @return string_list of the names
 */
string_list *read_tables(void);


/** 
 * Type definition for a table as read from a file
 * columns - the number of initial table's columns 
 * rows - the number of initial table's rows
 * array - an rows x columns array
 */
typedef struct
{
	uint32_t table_id;
    uint64_t rows;
    uint64_t columns;
    uint64_t **array;
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
 * Reads a table from a file
 * @param table - an (already allocated) table
 * @param filename - name of the file to read from
 * @return 0 for success, <0 for error
 */
int table_from_file(table *, char *);

//TODO: Old version (possibly remove it)
/**
 * Reads a table from a file
 *
 * @param filename - path of the file
 * @return table in table * format, NULL for error
 */
table *read_from_file(char *);


/**
 * Frees the momory used by the table
 * @param table*
 */
void delete_table(table*);

/**
 * Frees the momory used by the contents of a table (doesn't free the table itself)
 * @param table*
 */
void delete_table_contents(table*);

/**
 * Takes a list of table names and reads the tables from their files into a table_index struct
 * @param list - list of filenames
 * @return the tables in table_index format
 */
table_index *insert_tables_from_list(string_list *);


/**
 * Finds a table based on its id
 * @param table_index - a table_index struct that holds the tables
 * @param id - id of the table to find
 # @return pointer to the table, NULL if not found
 */
table *get_table(table_index *, uint32_t);

/**
 * Deletes a table_index and all its tables
 * @param table_index the table index
 */
void delete_table_index(table_index *);

/**
 * Reads in the tables from the files given from stdin
 * @return the tables in table_index format
 */
 table_index *insert_tables(void);

#endif	// TABLE_H