#ifndef LIST_ARRAY_H
#define LIST_ARRAY_H
#include "middle_list.h"

#define NUM_LISTS 3

typedef struct list_array
{
	middle_list *lists[NUM_LISTS][2];	//Or is it jobs?

} list_array;

list_array *create_list_array();
middle_list *append_middle_list(middle_list *, middle_list *);
void merge_middle_lists(list_array *, middle_list *, middle_list *);
void delete_list_array(list_array *);

#endif //LIST_ARRAY_H