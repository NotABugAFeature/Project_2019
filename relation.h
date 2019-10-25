#include <stdint.h>

/** 
 * Type definition for a tuple
 * key - key of sorting and join operation
 * row_id - incremental id of the tuple
 */
typedef struct
{
    int64_t key;
    int64_t row_id;
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


/**
 * Reads a table from a file
 *
 * @param filename - path of the file
 */
int64_t **read_from_file(char *);

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
int create_relation_from_table(int64_t* ,uint64_t , relation*);

/**
 * Prints all the tuples of the relation given
 * 
 * @param rel* The relation to print
 */
void print_relation(relation*);

