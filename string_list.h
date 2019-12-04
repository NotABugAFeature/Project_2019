#ifndef STRING_LIST_H
#define STRING_LIST_H

#define STRING_SIZE 512

/**
 * Type definition for a node of the string list
 * string - string held in the node
 * next - pointer to next node in list
 */
typedef struct string_list_node
{
	char string[STRING_SIZE];
	struct string_list_node *next;
} string_list_node;


/**
 * Type definition for a list of strings
 * num_nodes - number of nodes in list
 * head - pointer to start of list
 * tail - pointer to end of list
 */
typedef struct
{
	uint32_t num_nodes;
	string_list_node *head;
	string_list_node *tail;
} string_list;

/**
 * Creates a string_list
 * @return a string_list, NULL for error
 */
string_list *string_list_create(void);

/**
 * Inserts a string in a string_list
 *
 * @param list - an existing list
 * @param string - string to insert
 * @return 0 for success, <0 for error
 */
int string_list_insert(string_list *, char *);

/**
 * Removes the first string from a string_list
 * @param list - an existing list
 * @return the first string
 */
char *string_list_remove(string_list *);

/**
 * Prints a string_list
 * @param list - list to print
 */
void string_list_print(string_list *);

#endif // STRING_LIST_H