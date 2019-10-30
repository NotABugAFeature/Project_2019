/******************************************************************************/
/*  Application         :    Project_2019                                     */
/*  File                :    result_list.h                                    */
/*  Author              :    Georgakopoulos Panagiotis 1115201600028          */
/*  Team Member         :    Karamhna Maria            1115201600059          */
/*  Team Member         :    Koursiounis Georgios      1115201600077          */
/*  Instructor          :    Sarantis Paskalis                                */
/*  All Tests Conducted At The University's Linux Machines                    */
/******************************************************************************/
/*  This File Contains The Declaration Of The Results List                    */

#ifndef RESULTLIST_H
#define RESULTLIST_H

#include <stdint.h>

#define RESULT_LIST_BUCKET_SIZE 1048576/(2*sizeof(uint64_t))
#define ROWID_R_INDEX 0
#define ROWID_S_INDEX 1

typedef struct result_list_bucket result_list_bucket;
typedef struct result_list_node result_list_node;
typedef struct result_list result_list;

/**
 * Creates an empty result list and returns a pointer to that list
 * @return result_list* The new list
 */
result_list* create_result_list();
/**
 * Deletes all the nodes of the list given
 * @param result_list* the list to delete
 */
void delete_result_list(result_list*);
/**
 * Adds a pair of row ids in the last bucket of the list and creates a new
 * bucket if needed.
 * @param result_list* The list to add the row ids
 * @param uint64_t r_row_id
 * @param uint64_t s_row_id
 * @return int 0 If Successful
 */
int append_to_list(result_list*, uint64_t r_row_id, uint64_t s_row_id);
/**
 * Prints All The Nodes And Buckets Of The List From First To Last.
 * @param result_list The result list to print
 */
void print_result_list(result_list*);
/**
 * Returns if the list is empty.
 * @param result_list The res
 * @return int 1 if empty else 0
 */
int is_result_list_empty(result_list list);
/**
 * Returns the number of nodes in the result list.
 * @param
 * @return int The number of nodes
 */
int result_list_get_number_of_buckets(result_list);
#endif /*CLIENTLIST_H*/
