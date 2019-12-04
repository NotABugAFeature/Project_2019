#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include "middle_list.h"
#include "table.h"
#include "query.h"

int main(void)
{

	// FILE *ff = stdout;
	// middle_list* dd = create_middle_list();
	// append_to_middle_list(dd, 2);
	// append_to_middle_list(dd, 3);
	// append_to_middle_list(dd, 4);
	// append_to_middle_list(dd, 5);
	// append_to_middle_list(dd, 6);
	// print_middle_list(dd, ff);

	table_index table;
	table.num_tables = 3;
	table.tables = malloc(3*sizeof(table));

	table.tables[0].table_id = 0;
	table.tables[0].rows = 10;
	table.tables[0].columns = 2;

		int N = 10, M = 2;
	table.tables[0].array  = (uint64_t **)malloc(N * sizeof(uint64_t *));
   for (int i=0 ; i < N ; i++)
	 {
		 *(table.tables[0].array+i) = malloc(M * sizeof(uint64_t));
	 }

	 // for (int i=0 ; i < N ; i++)
	 // {
		//  for (int j=0 ; j < M ; j++)
		//  		table.tables[0].array[i][j]=i;
	 // }

	 for (int i=0 ; i < N ; i++)
	 {
		 	table.tables[0].array[i][0]=i;
			table.tables[0].array[i][1]=i+1;
	 }

	 for (int i=0 ; i < N ; i++)
	 {
		 for (int j=0 ; j < M ; j++)
		 		printf("%d  ", table.tables[0].array[i][j]);
			printf("\n");
	 }
/*************************/
table.tables[1].table_id = 1;
table.tables[1].rows = 10;
table.tables[1].columns = 2;


table.tables[1].array  = (uint64_t **)malloc(N * sizeof(uint64_t *));
for (int i=0 ; i < N ; i++)
{
 *(table.tables[1].array+i) = malloc(M * sizeof(uint64_t));
}

// for (int i=0 ; i < N ; i++)
// {
//  for (int j=0 ; j < M ; j++)
//  		table.tables[0].array[i][j]=i;
// }

for (int i=0 ; i < N ; i++)
{
	table.tables[1].array[i][0]=i;
	table.tables[1].array[i][1]=i+1;
}
printf("\n\n\n");
for (int i=0 ; i < N ; i++)
{
	for (int j=0 ; j < M ; j++)
		 printf("%d  ", table.tables[1].array[i][j]);
	 printf("\n");
}
/****************/


		query q;
		q.number_of_tables = 2;
		q.table_ids = malloc((q.number_of_tables)*sizeof(uint32_t));
		q.table_ids[0] = 0;
		q.table_ids[1] = 1;
		q.number_of_predicates = 1;
		q.predicates = malloc((q.number_of_predicates) * sizeof(predicate));

		//filter

		// q.predicates[0].type = Filter;
		// predicate_filter f;
		// f.filter_type = Greater_Equal;
		// f.value = 5;
		// f.r.table_id = 0;
		// f.r.column_id = 1;
		//
		// q.predicates[0].p = &f;

		q.predicates[0].type = Self_Join;
		predicate_join j;
		j.r.table_id = 0;
		j.r.column_id = 1;

		j.s.table_id = 0;
		j.s.column_id = 1;
		q.predicates[0].p = &j;

		bool b[2]={1,1};

		execute_query(&q, &table, b);

		free(q.predicates);

    return 0;
}
