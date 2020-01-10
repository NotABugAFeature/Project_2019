#ifndef MIDDLELIST_H
#define MIDDLELIST_H

#include <inttypes.h>

#define middle_LIST_BUCKET_SIZE (1048576/(2*sizeof(uint64_t)))
//#define middle_LIST_BUCKET_SIZE (262144/(sizeof(uint64_t)))

typedef struct middle_list_bucket middle_list_bucket;
typedef struct middle_list_node middle_list_node;
typedef struct middle_list middle_list;

/**
 * The Bucket inside a node of the middle list.
 * Contains a 2d array of row ids (uint64_t) and an index (unsigned int)
 * to the next empty space in the array.
 */
typedef struct middle_list_bucket
{
    uint64_t row_ids[middle_LIST_BUCKET_SIZE];
    unsigned int index_to_add_next;
} middle_list_bucket;

/**
 * The node of the middle list.
 * Contains a bucket with the row ids and a pointer to the next node.
 */
typedef struct middle_list_node
{
    middle_list_bucket bucket;
    middle_list_node* next;
} middle_list_node;

/**
 * The middle list.
 * Contains pointers to the head and tail nodes (for O(1) append)
 * and a node counter.
 */
typedef struct middle_list
{
    middle_list_node* head; //The first node of the list
    middle_list_node* tail; //The last node of the list
    unsigned int number_of_nodes; //Counter of the buckets;
    unsigned int number_of_records; //Counter of the records;
} middle_list;

/**
 * Creates and initializes a new empty middle list node (with the bucket)
 * @return middle_list_node* The new node
 */
struct middle_list_node* create_middle_list_node();

/**
 * Checks if the bucket given is full
 * @param middle_list_bucket The bucket to check
 * @return int 1 if the bucket is full else 0
 */
int is_middle_list_bucket_full(middle_list_bucket*);

/**
 * Appends the rowids given to the bucket.
 * @param middle_list_bucket* the bucket
 * @param uint64_t r_row_id
 * @return 0 if successful 1 else
 */
int append_to_middle_bucket(middle_list_bucket*, uint64_t);

/**
 * Prints the contents of the bucket (index and array)
 * @param middle_list_bucket the bucket to print
 * @param FILE* Where the output will be printed
 */
void print_middle_bucket(middle_list_bucket*,FILE*);

/**
 * Creates an empty middle list and returns a pointer to that list
 * @return middle_list* The new list
 */
middle_list* create_middle_list();


/**
 * Deletes all the nodes of the list given
 * @param middle_list* the list to delete
 */
void delete_middle_list(middle_list*);

/**
 * Adds a pair of row ids in the last bucket of the list and creates a new
 * bucket if needed.
 * @param middle_list* The list to add the row ids
 * @param uint64_t r_row_id
 * @return int 0 If Successful
 */
int append_to_middle_list(middle_list*, uint64_t r_row_id);


/**
 * Prints All The Nodes And Buckets Of The List From First To Last.
 * @param middle_list The middle list to print
 * @param FILE* Where the output will be printed
 */
void print_middle_list(middle_list*,FILE*);


/**
 * Returns if the list is empty.
 * @param middle_list The res
 * @return int 1 if empty else 0
 */
int is_middle_list_empty(middle_list* list);


/**
 * Returns the number of nodes in the middle list.
 * @param middle_list The middle list
 * @return int The number of nodes
 */
unsigned int middle_list_get_number_of_buckets(middle_list*);


/**
 * Returns the number of records in the middle list.
 * @param middle_list The middle list
 * @return uint64_t The number of records
 */
uint64_t middle_list_get_number_of_records(middle_list*);
typedef struct lookup_table
{
    middle_list_bucket **lookup_table;  //Pointers to the list nodes
    uint64_t size;                      //The number of nodes
    uint64_t* min;                      //The min rowid in each node
    uint64_t* max;                      //The max rowid in each node
}lookup_table;
lookup_table *construct_lookup_table(middle_list*);
void delete_lookup_table (lookup_table*);
//middle_list_bucket **construct_lookup_table(middle_list*);
#endif /*MIDDLELIST_H*/
