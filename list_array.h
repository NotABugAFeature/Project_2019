#ifndef LIST_ARRAY_H
#define LIST_ARRAY_H
#include "middle_list.h"

typedef struct list_array
{
	unsigned int num_lists;
	middle_list ***lists;	//Will be allocated as lists *[num_lists][2]

} list_array;

/**
 * Creates a list_array (with middle_lists initialized with create_middle_list)
 * @param unsigned int size - the number of lists in the array
 * @return the list_array, NULL for error
 */
list_array *create_list_array(unsigned int);

/**
 * Appends the second middle list given, to the end of the first middle list given (possibly leaving gaps in the last bucket)
 * @param middle_list *main_list - the list to append to
 * @param middle_list *list - the list to append to the main_list
 */
void append_middle_list(middle_list *, middle_list *);

/**
 * Appends the second middle list given, to the end of the first middle list given (leaving no gaps in the buckets)
 * @param middle_list *main_list - the list to append to
 * @param middle_list *list - the list to append to the main_list
 */
void append_middle_list_no_gaps(middle_list *, middle_list *);

/**
 * Merges all the lists of the list_array into two lists (one for R [0], one for S [1])
 * @param list_array *la - the list_array
 * @param middle_list *final_r - list to place the merged [0] lists (must be initialized with create_middle_list)
 * @param middle_list *final_s - list to place the merged [1] lists (must be initialized with create_middle_list)
 */
void merge_middle_lists(list_array *, middle_list *, middle_list *);

/**
 * Frees a list_array
 * @param list_array *la - the list_array
 */
void delete_list_array(list_array *);

#endif //LIST_ARRAY_H