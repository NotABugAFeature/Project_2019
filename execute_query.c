#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include <stdbool.h>
#include "sort_merge_join.h"
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
                         middle_list *new_list)
{
  // printf("rows %d cols %d\n", table->rows, table->columns);
  // for(int i=0;i<table->rows;i++)
  // {
  //   for(int j=0; j<table->columns;j++)
  //     printf("%d  ", table->array[i][j]);
  //   printf("\n");
  // }
printf("value: %d , filter %d, COLUMN: %d\n", filter->value, filter->filter_type, filter->r.column_id);

  for(unsigned int i = 0; i < bucket->index_to_add_next; i++)
  {
printf("bucket->row_ids[i]: %d\n", bucket->row_ids[i]);
printf("PROCESSING: %d\n", table->array[bucket->row_ids[i]][filter->r.column_id]);
    if( (filter->filter_type == Less && table->array[bucket->row_ids[i]][filter->r.column_id] < filter->value)||
        (filter->filter_type == Less_Equal && table->array[bucket->row_ids[i]][filter->r.column_id] <= filter->value)||
        (filter->filter_type == Equal && table->array[bucket->row_ids[i]][filter->r.column_id] == filter->value)||
        (filter->filter_type == Greater && table->array[bucket->row_ids[i]][filter->r.column_id] > filter->value)||
        (filter->filter_type == Greater_Equal && table->array[bucket->row_ids[i]][filter->r.column_id] >= filter->value)||
        (filter->filter_type == Not_Equal && table->array[bucket->row_ids[i]][filter->r.column_id] != filter->value)
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
                          middle_list *new_list)
{

  for(unsigned int i = 0; i < table->rows; i++)
  {
    if( (filter->filter_type == Less && table->array[i][filter->r.column_id] < filter->value)||
        (filter->filter_type == Less_Equal && table->array[i][filter->r.column_id] <= filter->value)||
        (filter->filter_type == Equal && table->array[i][filter->r.column_id] == filter->value)||
        (filter->filter_type == Greater && table->array[i][filter->r.column_id] > filter->value)||
        (filter->filter_type == Greater_Equal && table->array[i][filter->r.column_id] >= filter->value)||
        (filter->filter_type == Not_Equal && table->array[i][filter->r.column_id] != filter->value)
      )
    {
      if(append_to_middle_list(new_list, i))
        return 1;
    }
  }
  return 0;
}


relation *construct_relation_from_table(table * table, uint64_t column_id)
{
  relation *rel = malloc(sizeof(relation));
  if(rel == NULL)
  {
    //Error
    return NULL;
  }

  rel->num_tuples = table->rows;
  rel->tuples = malloc(table->rows*sizeof(tuple));
  if(rel == NULL)
  {
    //Error
    free(rel);
    return NULL;
  }

  for(unsigned int i = 0; i < table->rows; i++)
  {
  //  printf("%d\n", table->array[i][column_id]);
    rel->tuples[i].key = table->array[i][column_id];
    rel->tuples[i].row_id = i;
  }

  return rel;
}


void construct_relation_from_middleman(middle_list_bucket *bucket,
                                       table *table,
                                       relation *rel,
                                       uint64_t column_id,
                                       uint64_t *counter)
{
  for(unsigned int i = 0; i < bucket->index_to_add_next; i++)
  {
    rel->tuples[*counter].key = table->array[bucket->row_ids[i]][column_id];
    rel->tuples[*counter].row_id = i;

    (*counter)++;
  }
}

/* TODO : not sure if this is correct */
int update_middle_bucket(middle_list_bucket **lookup, middle_list_bucket *bucket, middle_list *updated_list)
{
  for(unsigned int i = 0; i < bucket->index_to_add_next; i++)
  {
    middle_list_bucket *target = lookup[(bucket->row_ids[i])/middle_LIST_BUCKET_SIZE];
    //check append
    append_to_middle_list(updated_list, target->row_ids[(bucket->row_ids[i])%middle_LIST_BUCKET_SIZE]);
  }
  return 0;
}


int self_join_table(predicate_join *join, table* table, middle_list *list)
{
	for(unsigned int i = 0; i < table->rows; i++)
	{
		if(table->array[i][join->r.column_id] == table->array[i][join->s.column_id])
		{
			if(append_to_middle_list(list, i))
				return 1;
		}
	}

  return 0;
}


int self_join_middle_bucket(predicate_join *join,
													  table *table_r,
	 													table *table_s,
														middle_list_bucket *bucket_r,
														middle_list_bucket *bucket_s,
														middle_list *list_r,
														middle_list *list_s)
{
	for(unsigned int i = 0; i < bucket_r->index_to_add_next; i++)
  {
		if(table_r->array[bucket_r->row_ids[i]][join->r.column_id] ==
					table_s->array[bucket_s->row_ids[i]][join->s.column_id])
		{
			//check
			if(append_to_middle_list(list_r, bucket_r->row_ids[i]) ||
				 append_to_middle_list(list_s, bucket_s->row_ids[i])
			  )
			{
				return 1;
			}
		}
  }
  return 0;
}


middleman *execute_query(query *q, table_index* index, bool *sorting)
{
  int bool_counter = 0;
	uint32_t concatenated_tables[q->number_of_predicates][q->number_of_tables+1];
	for(int i = 0; i < q->number_of_predicates; i++)
			concatenated_tables[i][0] = 0;

  //check for argument format
  if(q == NULL /*|| tables == NULL*/)
  {
    fprintf(stderr, "Null parameters\n");
    return NULL;
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
	//
	//
  // m->tables[1].list = create_middle_list();
	// append_to_middle_list(m->tables[1].list , 0);
	// append_to_middle_list(m->tables[1].list , 1);
	// append_to_middle_list(m->tables[1].list , 2);
  // append_to_middle_list(m->tables[1].list , 3);
  // append_to_middle_list(m->tables[1].list , 4);
  // append_to_middle_list(m->tables[1].list , 5);
  // append_to_middle_list(m->tables[1].list , 6);
  // append_to_middle_list(m->tables[1].list , 7);
  // append_to_middle_list(m->tables[1].list , 8);
  // append_to_middle_list(m->tables[1].list , 9);
	//print_middle_list(m->tables[0].list, ff);
printf("ALL OK\n");

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
        filter_original_table(filter, original_table, m->tables[filter->r.table_id].list);
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
          filter_middle_bucket(filter, &(list_temp->bucket), original_table, new_list);
          list_temp = list_temp->next;
        }

        printf("\n\n\n\n");
        print_middle_list(new_list, ff);
        delete_middle_list(m->tables[filter->r.table_id].list);
        m->tables[filter->r.table_id].list = new_list;
      }
    }
    else if(q->predicates[i].type == Join || (q->predicates[i].type == Self_Join &&
				(((predicate_join *)q->predicates[i].p)->r.table_id != ((predicate_join *)q->predicates[i].p)->s.table_id)))
    {
      predicate_join *join = q->predicates[i].p;

printf("SELF JOIN !=\n");

			//create middle lists
			middle_list *result_R = create_middle_list();
			if(result_R == NULL)
			{
				fprintf(stderr, "Error in create_result_list\n");
				return NULL;
			}

			middle_list *result_S = create_middle_list();
			if(result_S == NULL)
			{
				fprintf(stderr, "Error in create_result_list\n");
				return NULL;
			}


			//find original table r
			table *table_r = get_table(index, q->table_ids[join->r.table_id]);
			if(table_r == NULL)
			{
				printf("not found\n");
			}

			//find original tables
			table *table_s = get_table(index, q->table_ids[join->s.table_id]);
			if(table_s == NULL)
			{
				printf("not found\n");
			}

			if(q->predicates[i].type == Join)
			{printf("HHHHH\n");
	      //construct relation relR
	      //check if table exists in middleman.
	      //if yes use it else, if not take if from the original table
	      relation *relR;
	      if(m->tables[join->r.table_id].list == NULL)
	      {
	        printf("COLUMN %d\n", join->r.column_id);
	        relR = construct_relation_from_table(table_r, join->r.column_id);
	        printf("ok ok\n");
	      }
	      else
	      {
	        relR = malloc(sizeof(relation));
	        if(relR == NULL)
	        {
	          //Error
	          return NULL;
	        }

	        relR->num_tuples = middle_list_get_number_of_records(m->tables[join->r.table_id].list);
	        relR->tuples = malloc((relR->num_tuples)*sizeof(tuple));
	        if(relR == NULL)
	        {
	          //Error
	          free(relR);
	          return NULL;
	        }

	        uint64_t counter = 0;
	        middle_list_node *list_temp = m->tables[join->r.table_id].list->head;
	        while(list_temp != NULL)
	        {
	          construct_relation_from_middleman(&(list_temp->bucket), table_r, relR, join->r.column_id, &counter);
	          list_temp = list_temp->next;
	        }
	      }

	      for(int i=0; i<relR->num_tuples;i++)
	      {
	        printf("%d %d\n", relR->tuples[i].key, relR->tuples[i].row_id);
	      }


	      //construct relation relS
	      //check if table exists in middleman.
	      //if yes use it else, if not take if from the original table
	      relation *relS;
	      if(m->tables[join->s.table_id].list == NULL)
	      {
	        printf("COLUMN %d\n", join->s.column_id);
	        relS = construct_relation_from_table(table_s, join->s.column_id);
	        printf("ok ok\n");
	      }
	      else
	      {
	        relS = malloc(sizeof(relation));
	        if(relS == NULL)
	        {
	          //Error
	          return NULL;
	        }

	        relS->num_tuples = middle_list_get_number_of_records(m->tables[join->s.table_id].list);
	        relS->tuples = malloc((relS->num_tuples)*sizeof(tuple));
	        if(relS == NULL)
	        {
	          //Error
	          free(relS);
	          return NULL;
	        }

	        uint64_t counter = 0;
	        middle_list_node *list_temp = m->tables[join->s.table_id].list->head;
	        while(list_temp != NULL)
	        {
	          construct_relation_from_middleman(&(list_temp->bucket), table_s, relS, join->s.column_id, &counter);
	          list_temp = list_temp->next;
	        }
	      }

	      for(int i=0; i<relS->num_tuples;i++)
	      {
	        printf("%d %d\n", relS->tuples[i].key, relS->tuples[i].row_id);
	      }


	      //relR and RelS ready..check and sort
	      if(sorting[bool_counter] == 1)
	      {
	        if(radix_sort(relR))
	        {
		         fprintf(stderr, "Error in radix_sort\n");
		         return NULL;
	         }
	      }

	      if(sorting[bool_counter+1] == 1)
	      {
	        if(radix_sort(relS))
	        {
		         fprintf(stderr, "Error in radix_sort\n");
		         return NULL;
	         }
	      }

	      //update counter
	      bool_counter += 2;

	      printf("\n\n\nBEFORE JOIN\n\n\nTABLE R\n\n");
	      print_middle_list(m->tables[join->r.table_id].list, ff);
	      printf("\n\nTABLE S\n\n");
	      print_middle_list(m->tables[join->s.table_id].list, ff);

	      //finally join
	      if(final_join(result_R, result_S, relR, relS))
	      {
	          fprintf(stderr, "Error in final_join\n");
	          return NULL;
	      }

	      printf("\n\n\nAFTER JOIN\n\n\nRESULT R\n\n");
	      print_middle_list(result_R, ff);
	      printf("\n\nRESULT S\n\n");
	      print_middle_list(result_S, ff);

				//now go back to middleman
				if(m->tables[join->r.table_id].list == NULL)
				{printf("EMPTY1\n" );
					//if empty then rowid are those of the original table..store them
					m->tables[join->r.table_id].list = result_R;
				}
				else
				{
						//create new list
						middle_list *new_list = create_middle_list();

						middle_list_bucket **lookup = construct_lookup_table(m->tables[join->r.table_id].list);
						//get the new list
						middle_list_node *list_temp = result_R->head;
						while(list_temp != NULL)
						{
							//TODO check return
							update_middle_bucket(lookup, &(list_temp->bucket), new_list);
							list_temp = list_temp->next;
						}

						print_middle_list(new_list, ff);
						delete_middle_list(m->tables[join->r.table_id].list);
						m->tables[join->r.table_id].list = new_list;
				}


				if(m->tables[join->s.table_id].list == NULL)
				{printf("EMPTY2\n" );
					//if empty then rowid are those of the original table..store them
					m->tables[join->s.table_id].list = result_S;
				}
				else
				{
					//create new list
					middle_list *new_list = create_middle_list();

					middle_list_bucket **lookup = construct_lookup_table(m->tables[join->s.table_id].list);
					//get the new list
					middle_list_node *list_temp = result_S->head;
					while(list_temp != NULL)
					{
						//TODO check return
						update_middle_bucket(lookup, &(list_temp->bucket), new_list);
						list_temp = list_temp->next;
					}

					print_middle_list(new_list, ff);
					delete_middle_list(m->tables[join->s.table_id].list);
					m->tables[join->s.table_id].list = new_list;
				}



				printf("\n\n\nAFTER UPDATE\n\n\nTABLE R\n\n");
				print_middle_list(m->tables[join->r.table_id].list, ff);
				printf("\n\nTABLE S\n\n");
				print_middle_list(m->tables[join->s.table_id].list, ff);

			}
			else
			{
printf("SELF JOIN PROCESS: %d %d\n", join->r.table_id, join->s.table_id);
print_middle_list(m->tables[join->r.table_id].list, ff);
print_middle_list(m->tables[join->s.table_id].list, ff);
printf("\n\n\n");
				if(m->tables[join->r.table_id].list == NULL || m->tables[join->s.table_id].list == NULL)
				{
					fprintf(stderr, "Indirect self-join failed\n");
					return NULL;
				}

        middle_list_node *list_temp_r = m->tables[join->r.table_id].list->head;
				middle_list_node *list_temp_s = m->tables[join->s.table_id].list->head;
        while(list_temp_r != NULL)
        {printf("hello\n");
          //TODO check return
          self_join_middle_bucket(join, table_r, table_s, &(list_temp_r->bucket), &(list_temp_s->bucket), result_R, result_S);
          list_temp_r = list_temp_r->next;
					list_temp_s = list_temp_s->next;
        }

        delete_middle_list(m->tables[join->r.table_id].list);
        m->tables[join->r.table_id].list = result_R;

				delete_middle_list(m->tables[join->s.table_id].list);
				m->tables[join->s.table_id].list = result_S;


				print_middle_list(m->tables[join->r.table_id].list, ff);
				print_middle_list(m->tables[join->s.table_id].list, ff);
			}
/****************************************************************************************************/

///////////////////STABLE UP TO THIS POINT
      //now we update the rest of the concatenated lists..3rd code copy paste
      for(int k = 0; k < q->number_of_predicates; k++)
      {
				if(concatenated_tables[k][0] == 0)
					continue;

        //swipe row - if flag=1 then we found something in this row
        int flag = 0;
        for(int l = 1; l <= concatenated_tables[k][0]; l++)
        {
          if(concatenated_tables[k][l] == join->r.table_id || concatenated_tables[k][l] == join->s.table_id)
          {
            flag = 1;
            break;
          }
        }

        //if r or s exists then update
        if(flag){

          for(int l = 1; l <= concatenated_tables[k][0]; l++)
          {
            //update all relations except for the s.table_id
            if(concatenated_tables[k][l] == join->r.table_id || concatenated_tables[k][l] == join->s.table_id)
            	continue;

            //create new list
            middle_list *new_list = create_middle_list();

            middle_list_bucket **lookup = construct_lookup_table(m->tables[concatenated_tables[k][l]].list);
            //get the new list
            middle_list_node *list_temp = result_R->head;
            while(list_temp != NULL)
            {
              //TODO check return
              update_middle_bucket(lookup, &(list_temp->bucket), new_list);
              list_temp = list_temp->next;
            }

            print_middle_list(new_list, ff);
            delete_middle_list(m->tables[concatenated_tables[k][l]].list);
            m->tables[concatenated_tables[k][l]].list = new_list;

          }//endfor

        }//endflag

      }

			printf("\n\n\nPRINT CONCAT\n\n");
			for(int k = 0; k < q->number_of_predicates; k++)
			{
				for(int l = 0; l < q->number_of_tables+1; l++)
					printf("%d ", concatenated_tables[k][l]);
				printf("\n");
			}

			//find place of r and s
			int r_position = -1, s_position = -1;
			for(int k = 0; k < q->number_of_predicates; k++)
      {
				if(concatenated_tables[k][0] == 0)
					continue;

        //swipe row - if flag=1 then we found something in this row
        for(int l = 1; l <= concatenated_tables[k][0]; l++)
        {
          if(concatenated_tables[k][l] == join->r.table_id)
            r_position = k;
					else if(concatenated_tables[k][l] == join->s.table_id)
						s_position = k;
        }
			}
			printf("\n\nposition r %d, position s  %d\n", r_position, s_position);


			//neither of them exist
			if(r_position == -1 && s_position == -1)
			{
				for(int m = 0; m < q->number_of_predicates; m++)
				{
					if(concatenated_tables[m][0] == 0)
					{
						concatenated_tables[m][1] = join->r.table_id;
						concatenated_tables[m][2] = join->s.table_id;
						concatenated_tables[m][0] = 2;
					}
				}
			}
			else if(r_position == -1 && s_position != -1)
			{
				concatenated_tables[s_position][0]++;
				concatenated_tables[s_position][concatenated_tables[s_position][0]] = join->r.table_id;
			}
			else if(r_position != -1 && s_position == -1)
			{
				concatenated_tables[r_position][0]++;
				concatenated_tables[r_position][concatenated_tables[r_position][0]] = join->s.table_id;
			}
			else if(r_position != s_position)
			{
				int length = concatenated_tables[s_position][0];
				for(int m = 1; m <= length; m++)
				{
					concatenated_tables[r_position][0]++;

					concatenated_tables[r_position][concatenated_tables[r_position][0]] = concatenated_tables[s_position][m];

					concatenated_tables[s_position][0]--;

				}
			}


			printf("\n\n\nPRINT CONCAT AFTER\n\n");
			for(int k = 0; k < q->number_of_predicates; k++)
			{
				for(int l = 0; l < q->number_of_tables+1; l++)
					printf("%d ", concatenated_tables[k][l]);
				printf("\n");
			}


      //free synolikaa
    }
		else if(q->predicates[i].type == Self_Join &&
				(((predicate_join *)q->predicates[i].p)->r.table_id == ((predicate_join *)q->predicates[i].p)->s.table_id))
		{ printf("SELF JOIN\n");
			predicate_join *join = q->predicates[i].p;

      //find original tables
			table *table = get_table(index, q->table_ids[join->r.table_id]);
      if(table == NULL)
      {
        printf("not found\n");
      }

			if(m->tables[join->r.table_id].list != NULL)
			{
				fprintf(stderr, "Direct self-join failed\n");
				return NULL;
			}

			m->tables[join->r.table_id].list = create_middle_list();
			//check
			self_join_table(join, table, m->tables[join->r.table_id].list);
			//hope the optimization works and we have no memory leaks

			print_middle_list(m->tables[join->r.table_id].list, ff);
		}
    else
    {
      fprintf(stderr, "Undefined predicate type\n");
      return NULL;
    }


  }

  return m;
}




void calculate_sum(projection p, middle_list_bucket *bucket, table *table, uint64_t *sum)
{
  for(unsigned int i = 0; i < bucket->index_to_add_next; i++)
  {
		*sum += table->array[bucket->row_ids[i]][p.column_to_project.column_id];
	}
}


void calculate_projections(query *q, table_index* index, middleman *m)
{
	for(uint32_t i = 0; i < q->number_of_projections; i++)
	{
		projection p = q->projections[i];

		if(m->tables[p.column_to_project.table_id].list == NULL)
		{
			printf("NULL\n");
		}

		table *original_table = get_table(index, q->table_ids[p.column_to_project.table_id]);
		if(original_table == NULL)
		{
			printf("not found\n");
		}
printf("ALL SET\n");
		uint64_t sum = 0;
		middle_list_node *list_temp = m->tables[p.column_to_project.table_id].list->head;
		while(list_temp != NULL)
		{
			//TODO check return
			calculate_sum(p, &(list_temp->bucket), original_table, &sum);
			list_temp = list_temp->next;
		}

		printf("%d\n", sum);
	}

}
