#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include <stdbool.h>
#include "query.h"
#include "middle_list.h"
#include "table.h"


/**
 * Finds a table based on its id
 * @param table_index - a table_index struct that holds the tables
 * @param id - id of the table to find
 # @return pointer to the table, NULL if not found
 */
table *get_table(table_index *ti, uint32_t id)
{
	for(int i=0; i<ti->num_tables; i++)
	{
		if(ti->tables[i].table_id == id)
		{
			return &(ti->tables[i]);
		}
	}
	return NULL;
}



typedef struct{
  middle_list *list;
}middle_table;

typedef struct{
  uint32_t number_of_tables;
  middle_table *tables;
}middleman;

middleman *initialize_middleman(uint32_t number_of_tables)
{
  middleman *m = malloc(sizeof(middleman));
  if(m == NULL)
  {
    fprintf(stderr, "Cannot allocate memory for middleman\n");
    return NULL;
  }

  m->number_of_tables = number_of_tables;

  m->tables = malloc(number_of_tables*sizeof(middle_table));
  if(m->tables == NULL)
  {
    fprintf(stderr, "Cannot allocate memory for middle_tables\n");
    return NULL;
  }

  for(int i = 0; i < m->number_of_tables; i++)
    m->tables[i].list = NULL;

  return m;
}


int filter_middle_bucket(predicate_filter *filter,
                         middle_list_bucket *bucket,
                         table* table,
                         uint32_t column_id,
                         middle_list *new_list)
{
  // printf("rows %d cols %d\n", table->rows, table->columns);
  // for(int i=0;i<table->rows;i++)
  // {
  //   for(int j=0; j<table->columns;j++)
  //     printf("%d  ", table->array[i][j]);
  //   printf("\n");
  // }
printf("value: %d , filter %d\n", filter->value, filter->filter_type);

  for(unsigned int i = 0; i < bucket->index_to_add_next; i++)
  {
printf("bucket->row_ids[i]: %d\n", bucket->row_ids[i]);
printf("PROCESSING: %d\n", table->array[bucket->row_ids[i]][column_id]);
    if( (filter->filter_type == Less && table->array[bucket->row_ids[i]][column_id] < filter->value)||
        (filter->filter_type == Less_Equal && table->array[bucket->row_ids[i]][column_id] <= filter->value)||
        (filter->filter_type == Equal && table->array[bucket->row_ids[i]][column_id] == filter->value)||
        (filter->filter_type == Greater && table->array[bucket->row_ids[i]][column_id] > filter->value)||
        (filter->filter_type == Greater_Equal && table->array[bucket->row_ids[i]][column_id] >= filter->value)||
        (filter->filter_type == Not_Equal && table->array[bucket->row_ids[i]][column_id] != filter->value)
      )
    { printf("appending %d \n", bucket->row_ids[i]);
      if(append_to_middle_list(new_list, bucket->row_ids[i]))
        return 1;
    }
  }
  return 0;
}


int filter_original_table(predicate_filter *filter,
                          table* table,
                          uint32_t column_id,
                          middle_list *new_list)
{

  for(unsigned int i = 0; i < table->rows; i++)
  {
    if( (filter->filter_type == Less && table->array[i][column_id] < filter->value)||
        (filter->filter_type == Less_Equal && table->array[i][column_id] <= filter->value)||
        (filter->filter_type == Equal && table->array[i][column_id] == filter->value)||
        (filter->filter_type == Greater && table->array[i][column_id] > filter->value)||
        (filter->filter_type == Greater_Equal && table->array[i][column_id] >= filter->value)||
        (filter->filter_type == Not_Equal && table->array[i][column_id] != filter->value)
      )
    {
      if(append_to_middle_list(new_list, i))
        return 1;
    }
  }
  return 0;
}


int execute_query(query *q, table_index* index)
{
  //check for argument format
  if(q == NULL /*|| tables == NULL*/)
  {
    fprintf(stderr, "Null parameters\n");
    return 1;
  }

  //initialize middleman
  middleman *m = initialize_middleman(q->number_of_tables);

  FILE *ff = stdout;
	// m->tables[0].list = create_middle_list();
	// append_to_middle_list(m->tables[0].list , 0);
	// append_to_middle_list(m->tables[0].list , 1);
	// append_to_middle_list(m->tables[0].list , 2);
  // append_to_middle_list(m->tables[0].list , 3);
  // append_to_middle_list(m->tables[0].list , 4);
  // append_to_middle_list(m->tables[0].list , 5);
  // append_to_middle_list(m->tables[0].list , 6);
  // append_to_middle_list(m->tables[0].list , 7);
  // append_to_middle_list(m->tables[0].list , 8);
  // append_to_middle_list(m->tables[0].list , 9);
	// print_middle_list(m->tables[0].list, ff);

  //execute every predicate sequentially
  for(int i = 0; i < q->number_of_predicates; i++)
  {
    //filter
    if(q->predicates[i].type == Filter)
    {
      predicate_filter *filter = q->predicates[i].p;

      //find original table
      table *original_table = get_table(index, q->table_ids[filter->r.table_id]);
      if(original_table == NULL)
      {
        printf("not found\n");
      }

      if(m->tables[filter->r.table_id].list == NULL)
      { printf("null\n");
        //empty list so we need to take the data from the original table
        m->tables[filter->r.table_id].list = create_middle_list();
        //TODO check return
        filter_original_table(filter, original_table, q->table_ids[filter->r.column_id], m->tables[filter->r.table_id].list);
        print_middle_list(m->tables[filter->r.table_id].list, ff);
      }
      else
      {
        //list not empty so we modify it

        //create new list
        middle_list *new_list = create_middle_list();

printf("all ok  %d\n", q->table_ids[filter->r.table_id]);
        middle_list_node *list_temp = m->tables[filter->r.table_id].list->head;
        while(list_temp != NULL)
        {
          //TODO check return
          filter_middle_bucket(filter, &(list_temp->bucket), original_table, q->table_ids[filter->r.column_id], new_list);
          list_temp = list_temp->next;
        }

        printf("\n\n\n\n");
        print_middle_list(new_list, ff);
        delete_middle_list(m->tables[i].list);
        m->tables[0].list = new_list;
      }
    }
    else if(q->predicates[i].type == Join)
    {
      predicate_join *join = q->predicates[i].p;
      //check if table exists in middleman. If yes use it else, if not take if from the original table
      if(m->tables[join->r.table_id].list == NULL)
      {

      }
    }
    else
    {
      fprintf(stderr, "Undefined predicate type\n");
      return 2;
    }


  }

  return 0;
}
