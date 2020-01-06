#include <stdio.h>
#include <stdlib.h>
#include "list_array.h"

list_array *create_list_array()
{
	list_array *la = malloc(sizeof(list_array));
	for(int i=0; i<NUM_LISTS; i++)
	{
		la->lists[i][0] = create_middle_list();
		la->lists[i][1] = create_middle_list();
	}

	return la;
}

middle_list *append_middle_list(middle_list *main_list, middle_list *list)
{
	main_list->tail->next = list->head;
	main_list->tail = list->tail;
	main_list->number_of_nodes += list->number_of_nodes;

	free(list);
}

void merge_middle_lists(list_array *la, middle_list *final_r, middle_list *final_s)
{
	for(int i=0; i<NUM_LISTS; i++)
	{
		append_middle_list(final_r, la->lists[i][0]);
		append_middle_list(final_s, la->lists[i][1]);
	}
}

void delete_list_array(list_array *la)
{
	for(int i=0; i<NUM_LISTS; i++)
	{
		delete_middle_list(la->lists[i][0]);
		delete_middle_list(la->lists[i][1]);
	}

	free(la);
}