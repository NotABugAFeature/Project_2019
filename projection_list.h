#ifndef PROJECTION_LIST_H
#define PROJECTION_LIST_H
#include <inttypes.h>
#include <pthread.h>
typedef struct projection_node projection_node;
//Sorted list that holds the projection results
typedef struct projection_list
{
    projection_node* head;      //The first node of the list
    projection_node* tail;      //The last node of the list
    uint32_t number_of_nodes;   //The number of nodes inside the list
    pthread_mutex_t mutex;      //Mutex used for accessing the list
}projection_list;
/**
 * Creates and initializes an empty projection list
 * @return projection_list* the new list created or NULL if an error occurred
 */
projection_list* create_projection_list(void);
/**
 * Deletes the projection list given
 * @param projection_list* the list to delete
 */
void delete_projection_list(projection_list*);
/**
 * Adds a projection result inside the sorted list using the query id and the
 * the projection index
 * @param projection_list* the list to add the item
 * @param uint64_t the query_id
 * @param uint32_t the number of projections of the query
 * @param uint32_t the projection index
 * @param uint64_t the projection result
 * @return int 0 if successful
 */
int append_to_projection_list(projection_list*, uint64_t, uint32_t, uint32_t, uint64_t);
/**
 * Prints the projection list in the stderr with color
 * @param projection_list* the list to print
 */
void print_projection_list(projection_list*);
#endif /* PROJECTION_LIST_H */
