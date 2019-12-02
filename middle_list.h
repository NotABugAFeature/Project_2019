#ifndef middleLIST_H
#define middleLIST_H

#include <inttypes.h>

#define middle_LIST_BUCKET_SIZE (1048576/(2*sizeof(uint64_t)))
#define ROWID_R_INDEX 0
#define ROWID_S_INDEX 1

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
} middle_list;


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
 * @param uint64_t s_row_id
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
 * @param
 * @return int The number of nodes
 */
unsigned int middle_list_get_number_of_buckets(middle_list*);


/**
 * Returns the number of records in the middle list.
 * @param
 * @return uint64_t The number of records
 */
uint64_t middle_list_get_number_of_records(middle_list*);

#endif /*CLIENTLIST_H*/
