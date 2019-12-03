#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include "relation.h"
#include "table.h"

int main(void)
{

	table_name_list *list = read_tables();
	printf("List of names:\n");
	table_name_list_print(list);

	table_index *ti = insert_tables_from_list(list);
	printf("ti->num_tables: %" PRIu64 "\n", ti->num_tables);
	for(int i=0; i<ti->num_tables; i++)
	{
		printf("ti->tables[%d].table_id: %" PRIu32 " - ti->tables[%d].columns: %" PRIu64 " - ti->tables[%d].rows: %" PRIu64 "\n", i, ti->tables[i].table_id, i, ti->tables[i].columns, i, ti->tables[i].rows);
	}

	uint32_t id;
	printf("Table to find: ");
	scanf("%" PRIu32, &id);
	table *t = get_table(ti, id);
	if(t == NULL)
	{
		printf("Nothing found :(\n");
	}
	else
	{
		printf("Found table with table_id: %" PRIu32 ", rows: %" PRIu64 "\n", t->table_id, t->rows);
	}
	
	delete_table_index(ti);

	return 0;
	/*
	table *t = table_from_file("r0");
	if(t == NULL) return -1;
	printf("Columns: %" PRIu64 ", rows: %" PRIu64 "\n", t->columns, t->rows);
    //printf("First two things: %" PRIu64 " - %" PRIu64 "\n", *(t->data), *(t->data+sizeof(uint64_t)));

    printf("first column:\n");
    for(int i=0; i<t->rows; i++)
    {
    	printf("%" PRIu64 "\n", t->array[1][i]);
    }

    delete_table(t);
*/



    return 0;
}