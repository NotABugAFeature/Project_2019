#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include "string_list.h"

/**
 * Creates a string_list
 * @return a string_list, NULL for error
 */
string_list *string_list_create(void)
{
	string_list *list = malloc(sizeof(string_list));
	if(list == NULL)
	{
		perror("string_list_create: malloc error");
		return NULL;
	}
	list->num_nodes = 0;
	list->head = NULL;
	list->tail = NULL;

	return list;
}


/**
 * Inserts a string in a string_list
 *
 * @param list - an existing list
 * @param string - string to insert
 * @return 0 for success, <0 for error
 */
int string_list_insert(string_list *list, char *string)
{
	string_list_node *node = malloc(sizeof(string_list_node));
	if(node == NULL)
	{
		perror("string_list_insert: malloc error");
		return -1;
	}
	strcpy(node->string, string);
	node->next = NULL;

	if(list->head == NULL)
	{
		list->head = node;
		list->tail = node;
	}
	else
	{
		list->tail->next = node;
		list->tail = node;
	}
	list->num_nodes++;

	return 0;
}


/**
 * Removes the first string from a string_list
 * @param list - an existing list
 * @return the first string
 */
char *string_list_remove(string_list *list)
{
	if(list->head == NULL)
	{
		return NULL;
	}

	string_list_node *node = list->head;
	if(list->head == list->tail)
	{
		list->tail = list->tail->next;
	}
	list->head = list->head->next;

	char *string = malloc(STRING_SIZE*sizeof(char));
	if(string == NULL)
	{
		perror("string_list_remove: malloc error");
		free(node);
		return NULL;
	}
	strcpy(string, node->string);
	free(node);
	list->num_nodes--;
	return string;
}


/**
 * Prints a string_list
 * @param list - list to print
 */
void string_list_print(string_list *list)
{
	string_list_node *node = list->head;
	while(node != NULL)
	{
		printf("%s\n", node->string);
		node = node->next;
	}
}