#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include "sort_merge_join.h"
#include "middle_list.h"
#include "query.h"

int main(void)
{

	FILE *ff = stdout;
	middle_list* dd = create_middle_list();
	append_to_middle_list(dd, 2);
	append_to_middle_list(dd, 3);
	append_to_middle_list(dd, 4);
	append_to_middle_list(dd, 5);
	append_to_middle_list(dd, 6);
	print_middle_list(dd, ff);

		query q;
		q.number_of_predicates = 1;
		q.predicates = malloc((q.number_of_predicates) * sizeof(predicate));

		q.predicates[0].type = Filter;

		predicate_filter f;
		f.filter_type = Less;
		q.predicates[0].p = &f;

		execute_query(&q);

		free(q.predicates);

    return 0;
}
