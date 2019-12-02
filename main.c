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

 uint64_t t[10][2];
 	t[0][0] = 103;
	t[0][1] = 234;
	t[1][0] = 9456;
	t[1][1] = 457;
	t[2][0] = 105;
	t[2][1] = 789;
	t[3][0] = 821;
	t[3][1] = 124;
	t[4][0] = 4769;
	t[4][1] = 357;
	t[5][0] = 487;
	t[5][1] = 102;
	t[6][0] = 324;
	t[6][1] = 30;
	t[7][0] = 565;
	t[7][1] = 78;
	t[8][0] = 2324;
	t[8][1] = 978;
	t[9][0] = 856;
	t[9][1] = 1000;

		int N = 10, M = 2;
	table.tables[0].array  = (uint64_t **)malloc(N * sizeof(uint64_t *));
   for (int i=0 ; i < N ; i++)
	 {
		 *(table.tables[0].array+i) = malloc(M * sizeof(uint64_t));
	 }

	 for (int i=0 ; i < N ; i++)
	 {
		 for (int j=0 ; j < M ; j++)
		 		table.tables[0].array[i][j]=i;
	 }

	 for (int i=0 ; i < N ; i++)
	 {
		 for (int j=0 ; j < M ; j++)
		 		printf("%d  ", table.tables[0].array[i][j]);
			printf("\n");
	 }


		query q;
		q.number_of_tables = 1;
		q.table_ids = malloc(sizeof(uint32_t));
		q.table_ids[0] = 0;
		q.number_of_predicates = 1;
		q.predicates = malloc((q.number_of_predicates) * sizeof(predicate));

		q.predicates[0].type = Filter;

		predicate_filter f;
		f.filter_type = Greater_Equal;
		f.value = 5;
		f.r.table_id = 0;
		f.r.column_id = 0;

		q.predicates[0].p = &f;

		execute_query(&q, &table);

		free(q.predicates);

    return 0;
}
