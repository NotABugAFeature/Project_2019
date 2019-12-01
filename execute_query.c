#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include <stdbool.h>
#include "query.h"
#include "middle_list.h"

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


int execute_query(query *q/*, table_index* tables*/)
{
  //check for argument format
  if(q == NULL /*|| tables == NULL*/)
  {
    fprintf(stderr, "Null parameters\n");
    return 1;
  }

  //initialize middleman
  middleman *m = initialize_middleman(q->number_of_tables);

  //execute every predicate sequentially
  for(int i = 0; i < q->number_of_predicates; i++)
  {
    //filter
    if(q->predicates[i].type == Filter)
    {
      predicate_filter *filter = q->predicates[i].p;

      if(m->tables[filter->r.table_id].list == NULL)
      {
        //empty list so we need to take the data from the original table
        m->tables[filter->r.table_id].list = create_middle_list();
      }
      else
      {
        //list not empty so we modify it
        middle_list *temp = create_middle_list();

        unsigned int index = 0;
        middle_list_node *list_temp = m->tables[filter->r.table_id].list->head;
        while(list_temp != NULL)
        {
            for(unsigned int j = 0; j < list_temp->bucket->index_to_add_next; j++)
            {
                //pinakas[list_temp->bucket->row_ids[j]][filter->r.column_id]
                if(filter->filter_type == Less)
                {
//append
                }
                else if(filter->filter_type == Less_Equal)
                {

                }
                else if(filter->filter_type == Equal)
                {

                }
                else if(filter->filter_type == Greater)
                {

                }
                else if(filter->filter_type == Greater_Equal)
                {

                }
                else if(filter->filter_type == Not_Specified)
                {

                }
            }

            index++;
            list_temp = list_temp->next;
        }
      }

      //((predicate_filter *) q->predicates[i].p)->value
    //  predicate_filter_type type = ((predicate_filter *) q->predicates[i].p)->filter_type;
    }
    else if(q->predicates[i].type == Join)
    {
      //check if table exists in middleman. If yes use it else, if not take if from the original table
      if(m->tables[((predicate_join *) q->predicates[i].p)->r.table_id].array == NULL)
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
