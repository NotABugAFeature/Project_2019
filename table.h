#ifndef TABLE_H
#define TABLE_H

#define FILENAME_SIZE 100

/* ---------- TABLE_NAME_LIST ---------- */

/**
 * Type definition for a node of the table name list
 * filename - name of the table file
 * next - pointer to next node in list
 */
typedef struct table_name_list_node
{
	char filename[FILENAME_SIZE];
	struct table_name_list_node *next;
} table_name_list_node;


/**
 * Type definition for a list of table names
 * num_nodes - number of nodes in list
 * head - pointer to start of list
 * tail - pointer to end of list
 */
typedef struct
{
	int num_nodes;
	table_name_list_node *head;
	table_name_list_node *tail;
} table_name_list;

/**
 * Creates a table_name_list
 * @return a table_name_list, NULL for error
 */
table_name_list *table_name_list_create(void);

/**
 * Inserts a table name in a table_name_list
 *
 * @param list - an existing list
 * @param filename - name to insert
 * @return 0 for success, <0 for error
 */
int table_name_list_insert(table_name_list *, char *);

/**
 * Removes the first name from a table_name_list
 * @param list - an existing list
 * @return the first filename
 */
char *table_name_list_remove(table_name_list *);

/**
 * Prints a table_name_list
 * @param list - list to print
 */
void table_name_list_print(table_name_list *);

/**
 * Reads filenames of tables from stdin and returns them in a list
 * @return table_name_list of the names
 */
table_name_list *read_tables(void);


/* ---------- TABLES ---------- */

/** 
 * Type definition for a table as read from a file
 * rows - the number of initial table's columns 
 * columns - the number of initial table's rows
 * array - an rows x columns array
 */
typedef struct
{
	uint32_t table_id;
    uint64_t rows;
    uint64_t columns;
    uint64_t **array;
    //uint64_t *data;
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
table_index *insert_tables_from_list(table_name_list *);

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