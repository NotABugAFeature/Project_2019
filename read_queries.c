#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "string_list.h"

/**
 * Reads queries from stdin and returns them in a list
 * @return string_list of the queries
 */
string_list *read_batch(void)
{
	char line[STRING_SIZE];
	string_list *list = string_list_create();

	while(1)
	{
		fgets(line, STRING_SIZE, stdin);
		if(line == NULL)
		{
			return NULL;
		}
		line[strlen(line) - 1] = '\0';
		if(strcmp(line, "F") == 0)
		{
			break;
		}
		string_list_insert(list, line);
	}

	return list;
}

void read_queries(void)
{
	string_list *list;
	while(1)
	{
		list = read_batch();
		if(list == NULL)
		{
			printf("Input ended\n");
			return;
		}
		//Call query analysis, execute queries

		////////////For now just print
		char *str;
		int n = list->num_nodes;
		printf("List:\n");
		for(int i=0; i<n; i++)
		{
			str = string_list_remove(list);
			printf("%s\n", str);
		}
		free(list);
		////////////
	}
}


int main(void)
{
	read_queries();
	printf("Done\n");
}