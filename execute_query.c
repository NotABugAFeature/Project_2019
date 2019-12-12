#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include <stdbool.h>
#include "sort_merge_join.h"
#include "query.h"
#include "middle_list.h"
#include "table.h"
#include "execute_query.h"


/**
 * Middleman initializator & memory allocator
 *
 * @param receives a number of tables
 * @return a middleman struct with 'number_of_tables' elements
 */
middleman *initialize_middleman(uint32_t number_of_tables)
{
  if(number_of_tables < 1)
  {
    fprintf(stderr, "initialize_middleman: Cannot create middleman with %d tables\n", number_of_tables);
    return NULL;
  }

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
    free(m);
    fprintf(stderr, "Cannot allocate memory for middle_tables\n");
    return NULL;
  }

  for(uint32_t i= 0; i < m->number_of_tables; i++)
  {
    m->tables[i].list = NULL;
  }

  return m;
}


/**
 * Traverses the elements of a bucket and filters them according to a value
 * The results are stored in a new list (new_list)
 *
 * @return 1 in case of error or 0 if successful
 */
int filter_middle_bucket(predicate_filter *filter,
                         middle_list_bucket *bucket,
                         table* table,
                         middle_list *new_list)
{
  for(uint32_t i = 0; i < bucket->index_to_add_next; i++)
  {
    if( (filter->filter_type == Less && table->array[filter->r.column_id][bucket->row_ids[i]] < filter->value)||
        (filter->filter_type == Less_Equal && table->array[filter->r.column_id][bucket->row_ids[i]] <= filter->value)||
        (filter->filter_type == Equal && table->array[filter->r.column_id][bucket->row_ids[i]] == filter->value)||
        (filter->filter_type == Greater && table->array[filter->r.column_id][bucket->row_ids[i]] > filter->value)||
        (filter->filter_type == Greater_Equal && table->array[filter->r.column_id][bucket->row_ids[i]] >= filter->value)||
        (filter->filter_type == Not_Equal && table->array[filter->r.column_id][bucket->row_ids[i]] != filter->value)
      )
    {
      if(append_to_middle_list(new_list, bucket->row_ids[i]))
        return 1;
    }
  }
  return 0;
}


/**
 * Traverses the elements of a table and filters them according to a value
 * The results are stored in a new list (new_list)
 *
 * @return 1 in case of error or 0 if successful
 */
int filter_original_table(predicate_filter *filter,
                          table* table,
                          middle_list *new_list)
{

  for(uint64_t i = 0; i < table->rows; i++)
  {
    if( (filter->filter_type == Less && table->array[filter->r.column_id][i] < filter->value)||
        (filter->filter_type == Less_Equal && table->array[filter->r.column_id][i] <= filter->value)||
        (filter->filter_type == Equal && table->array[filter->r.column_id][i] == filter->value)||
        (filter->filter_type == Greater && table->array[filter->r.column_id][i] > filter->value)||
        (filter->filter_type == Greater_Equal && table->array[filter->r.column_id][i] >= filter->value)||
        (filter->filter_type == Not_Equal && table->array[filter->r.column_id][i] != filter->value)
      )
    {
      if(append_to_middle_list(new_list, i))
        return 1;
    }
  }
  return 0;
}


/**
 * Constructs a 'relation' for the join operation based on the elements of a table
 *
 * @return a pointer to the relation or NULL
 */
relation *construct_relation_from_table(table * table, uint64_t column_id)
{
  relation *rel = malloc(sizeof(relation));
  if(rel == NULL)
  {
    fprintf(stderr, "construct_relation_from_table: Cannot allocate memory\n");
    return NULL;
  }

  rel->num_tuples = table->rows;
  rel->tuples = malloc((rel->num_tuples)*sizeof(tuple));
  if(rel == NULL)
  {
    fprintf(stderr, "construct_relation_from_table: Cannot allocate memory\n");
    return NULL;
  }

  for(uint64_t i = 0; i < rel->num_tuples; i++)
  {
    rel->tuples[i].key = table->array[column_id][i];
    rel->tuples[i].row_id = i;
  }

  return rel;
}


/**
 * Constructs a 'relation' for the join operation based on the elements of a bucket
 * The results are stored to a relation (rel)
 */
void construct_relation_from_middleman(middle_list_bucket *bucket,
                                       table *table,
                                       relation *rel,
                                       uint64_t column_id,
                                       uint64_t *counter)
{
  for(uint32_t i = 0; i < bucket->index_to_add_next; i++)
  {
    rel->tuples[(*counter)].key = table->array[column_id][bucket->row_ids[i]];
    rel->tuples[(*counter)].row_id = *counter;

    (*counter)++;
  }
}


/**
 * After join, we go back to the middleman and for every rowId of the result 
 * we find the equivalent content in the middleman.
 * The result is stored in a new list (updated_list)
 * 
 * @return 1 in case of error or 0 if successful
 */
int update_middle_bucket(middle_list_bucket **lookup, middle_list_bucket *bucket, middle_list *updated_list)
{
  for(uint32_t i = 0; i < bucket->index_to_add_next; i++)
  {
    middle_list_bucket *target = lookup[(bucket->row_ids[i])/middle_LIST_BUCKET_SIZE];

    if(target == NULL)
    {
      fprintf(stderr, "update_middle_bucket: Target bucket not found in lookup table\n");
      return 2;
    }

    if(append_to_middle_list(updated_list, target->row_ids[(bucket->row_ids[i])%middle_LIST_BUCKET_SIZE]))
      return 1;
  }
  return 0;
}


/**
 * Takes a table and performs a self-join based on two columns 
 * The rowIds of equal columns are stored in a list (list)
 * 
 * @return 1 in case of error or 0 if successful
 */
int self_join_table(predicate_join *join, table* table, middle_list *list)
{
  for(uint64_t i = 0; i < table->rows; i++)
  {
    if(table->array[join->r.column_id][i] == table->array[join->s.column_id][i])
    {
      if(append_to_middle_list(list, i))
        return 1;
    }
  }

  return 0;
}


/**
 * Indirect Self-join of two buckets in the middleman
 * The rowIds of equal columns are stored in two new lists(list_r, list_s)
 * 
 * @return 1 in case of error or 0 if successful
 */
int self_join_middle_bucket(predicate_join *join,
                            table *table_r,
                            table *table_s,
                            middle_list_bucket *bucket_r,
                            middle_list_bucket *bucket_s,
                            middle_list *list_r,
                            middle_list *list_s,
                            middle_list *index_list,
                            uint32_t *counter)
{
  for(uint32_t i = 0; i < bucket_r->index_to_add_next; i++)
  {
    if(table_r->array[join->r.column_id][bucket_r->row_ids[i]] ==
       table_s->array[join->s.column_id][bucket_s->row_ids[i]])
    {
      if(append_to_middle_list(list_r, bucket_r->row_ids[i]) ||
         append_to_middle_list(list_s, bucket_s->row_ids[i]) ||
         append_to_middle_list(index_list, *counter))
      {
        return 1;
      }
    }
    (*counter)++;
  }
  return 0;
}



int original_self_join_middle_bucket(predicate_join *join,
                         middle_list_bucket *bucket,
                         table* table,
                         middle_list *new_list)
{
  for(unsigned int i = 0; i < bucket->index_to_add_next; i++)
  {
    if(table->array[join->r.column_id][bucket->row_ids[i]] == table->array[join->s.column_id][bucket->row_ids[i]])
    {
      if(append_to_middle_list(new_list, bucket->row_ids[i]))
        return 1;
    }
  }
  return 0;
}



middleman *execute_query(query *q, table_index* index, bool *sorting)
{
  int bool_counter = 0;
  uint32_t concatenated_tables[q->number_of_predicates][q->number_of_tables+1];
  for(uint32_t i = 0; i < q->number_of_predicates; i++)
    concatenated_tables[i][0] = 0;

  //Check for proper argument format
  if(q == NULL || index == NULL || sorting == NULL)
  {
    fprintf(stderr, "execute_query: Null parameters\n");
    return NULL;
  }

  //Initialize middleman
  middleman *m = initialize_middleman(q->number_of_tables);


  //Execute every predicate sequentially
  for(uint32_t i = 0; i < q->number_of_predicates; i++)
  {
    //Case A. filter
    if(q->predicates[i].type == Filter)
    {
      predicate_filter *filter = q->predicates[i].p;

      //A.1 Find original table
      table *original_table = get_table(index, q->table_ids[filter->r.table_id]);
      if(original_table == NULL)
      {
        fprintf(stderr, "execute_query: Table not found\n");
        return NULL;
      }

      //A.2 Check if there is a middleman list
      if(m->tables[filter->r.table_id].list == NULL)
      {
        //A.2.1 If it is empty we need to filter the data of the original table..
        m->tables[filter->r.table_id].list = create_middle_list();
        if(filter_original_table(filter, original_table, m->tables[filter->r.table_id].list))
        {
          fprintf(stderr, "execute_query filter_original_table: Error\n");
          return NULL;
        }
      }
      else
      {
        //A.2.2 ..else we filter the data of the middleman's list
        if(m->tables[filter->r.table_id].list->number_of_nodes > 0)
        {

          middle_list *new_list = create_middle_list();

          //traverse existing list and store the new results in 'new_list'
          middle_list_node *list_temp = m->tables[filter->r.table_id].list->head;
          while(list_temp != NULL)
          {
            if(filter_middle_bucket(filter, &(list_temp->bucket), original_table, new_list))
            {
              fprintf(stderr, "execute_query filter_middle_bucket: Error\n");
              return NULL;
            }
            list_temp = list_temp->next;
          }

          //delete old list and put the new one in its place
          delete_middle_list(m->tables[filter->r.table_id].list);
          m->tables[filter->r.table_id].list = new_list;
        }
      }
    }
    //Case B. Original Join or Indirect Self-Join
    else if(q->predicates[i].type == Join || (q->predicates[i].type == Self_Join &&
        (((predicate_join *)q->predicates[i].p)->r.table_id != ((predicate_join *)q->predicates[i].p)->s.table_id)))
    {
      predicate_join *join = q->predicates[i].p;

      middle_list *index_list = NULL;
      int delete_r = 1, delete_s = 1;

      //B.1 Create middle lists in which we are going to store the join results
      middle_list *result_R = create_middle_list();
      if(result_R == NULL)
      {
        fprintf(stderr, "execute_query: Error in create_result_list\n");
        return NULL;
      }

      middle_list *result_S = create_middle_list();
      if(result_S == NULL)
      {
        fprintf(stderr, "execute_query: Error in create_result_list\n");
        return NULL;
      }


      //B.2 Find original tables
      table *table_r = get_table(index, q->table_ids[join->r.table_id]);
      if(table_r == NULL)
      {
        fprintf(stderr, "execute_query:  Null parameters\n");
        return NULL;
      }

      table *table_s = get_table(index, q->table_ids[join->s.table_id]);
      if(table_s == NULL)
      {
        fprintf(stderr, "execute_query:  Null parameters\n");
        return NULL;
      }


      //B.3 If type = Join..
      if(q->predicates[i].type == Join)
      {
        //B.3.1 Construct relation relR
        //Check if table exists in middleman
        //If yes use it else, if not take if from the original table
        relation *relR;
        if(m->tables[join->r.table_id].list == NULL)
        {
          relR = construct_relation_from_table(table_r, join->r.column_id);
        }
        else
        {
          //relR memory allocation
          relR = malloc(sizeof(relation));
          if(relR == NULL)
          {
            fprintf(stderr, "execute_query: Cannot allocate memory\n");
            return NULL;
          }

          relR->num_tuples = middle_list_get_number_of_records(m->tables[join->r.table_id].list);
          if(relR->num_tuples > 0)
          {
            relR->tuples = malloc((relR->num_tuples)*sizeof(tuple));
            if(relR->tuples == NULL)
            {
              fprintf(stderr, "execute_query: Cannot allocate memory\n");
              return NULL;
            }

            //Construct relation from middleman
            uint64_t counter = 0;
            middle_list_node *list_temp = m->tables[join->r.table_id].list->head;
            while(list_temp != NULL)
            {
              construct_relation_from_middleman(&(list_temp->bucket), table_r, relR, join->r.column_id, &counter);
              list_temp = list_temp->next;
            }
          }
        }

        //B.3.2 Construct relation relS
        //Check if table exists in middleman
        //If yes use it else, if not take if from the original table
        relation *relS;
        if(m->tables[join->s.table_id].list == NULL)
        {
          relS = construct_relation_from_table(table_s, join->s.column_id);
        }
        else
        {
          relS = malloc(sizeof(relation));
          if(relS == NULL)
          {
            fprintf(stderr, "execute_query: Cannot allocate memory\n");
            return NULL;
          }

          relS->num_tuples = middle_list_get_number_of_records(m->tables[join->s.table_id].list);
          if(relS->num_tuples > 0)
          {
            relS->tuples = malloc((relS->num_tuples)*sizeof(tuple));
            if(relS->tuples == NULL)
            {
              fprintf(stderr, "execute_query: Cannot allocate memory\n");
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
        }

        //B.3.3 relR and RelS are ready..Sort if necessary
        if(sorting[bool_counter] == 1)
        {
          if(radix_sort(relR))
          {
            fprintf(stderr, "execute_query: Error in radix_sort\n");
            return NULL;
          }
        }

        if(sorting[bool_counter+1] == 1)
        {
          if(radix_sort(relS))
          {
            fprintf(stderr, "execute_query: Error in radix_sort\n");
            return NULL;
          }
        }

        //Update counter
        bool_counter += 2;

        //B.3.4 Join
        if(final_join(result_R, result_S, relR, relS))
        {
          fprintf(stderr, "execute_query: Error in final_join\n");
          return NULL;
        }

        //B.3.5 Now go back to middleman
        //If the list exists then update it
        //If not then put the result lisr (result_R, result_S) in its place
        if(m->tables[join->r.table_id].list == NULL)
        {
          m->tables[join->r.table_id].list = result_R;
          delete_r = 0;
        }
        else
        {

          middle_list *new_list = create_middle_list();
          if(result_R->number_of_nodes > 0)
          {
            //Construct lookup table of the existing (old) list
            middle_list_bucket **lookup = construct_lookup_table(m->tables[join->r.table_id].list);

            //Traverse result list and for every rowId find it in the old list and put
            //the result in the new_list
            middle_list_node *list_temp = result_R->head;
            while(list_temp != NULL)
            {
              if(update_middle_bucket(lookup, &(list_temp->bucket), new_list))
              {
                fprintf(stderr, "execute_query: Error in update_middle_bucket\n");
                return NULL;
              }
              list_temp = list_temp->next;
            }

            free(lookup);
          }

          delete_middle_list(m->tables[join->r.table_id].list);
          m->tables[join->r.table_id].list = new_list;
        }


        //Same procedure for S as above
        if(m->tables[join->s.table_id].list == NULL)
        {
          m->tables[join->s.table_id].list = result_S;
          delete_s = 0;
        }
        else
        {
          middle_list *new_list = create_middle_list();
          if(result_S->number_of_nodes > 0)
          {
            middle_list_bucket **lookup = construct_lookup_table(m->tables[join->s.table_id].list);

            middle_list_node *list_temp = result_S->head;
            while(list_temp != NULL)
            {
              if(update_middle_bucket(lookup, &(list_temp->bucket), new_list))
              {
                fprintf(stderr, "execute_query: Error in update_middle_bucket\n");
                return NULL;
              }
              list_temp = list_temp->next;
            }

            free(lookup);
          }

          delete_middle_list(m->tables[join->s.table_id].list);
          m->tables[join->s.table_id].list = new_list;
        }

        //B.3.6 Free relations
        if(relR->num_tuples > 0)
          free(relR->tuples);
        free(relR);

        if(relS->num_tuples > 0)
          free(relS->tuples);
        free(relS);
      }
      //B.4 If type = (Indirect) Self-Join..
      //The indirect self-join is performed when both join members (r,s) are in the middleman
      else
      { 
        if(m->tables[join->r.table_id].list == NULL || m->tables[join->s.table_id].list == NULL)
        {
          fprintf(stderr, "execute_query: Indirect self-join failed\n");
          return NULL;
        }

        index_list = create_middle_list();

        middle_list_node *list_temp_r = m->tables[join->r.table_id].list->head;
        middle_list_node *list_temp_s = m->tables[join->s.table_id].list->head;

        //Traverse the two lists simultaneously and keep only the rowIds 
        //with the same content in the original tables
        uint32_t counter = 0;
        while(list_temp_r != NULL)
        {
          if(self_join_middle_bucket(join, table_r, table_s, &(list_temp_r->bucket), &(list_temp_s->bucket), result_R, result_S, index_list, &counter))
          {
            fprintf(stderr, "execute_query: Error in self_join_middle_bucket\n");
            return NULL;
          }
          list_temp_r = list_temp_r->next;
          list_temp_s = list_temp_s->next;
        }

        //Update the lists and delete the old ones
        delete_middle_list(m->tables[join->r.table_id].list);
        m->tables[join->r.table_id].list = result_R;
        delete_r = 0;

        delete_middle_list(m->tables[join->s.table_id].list);
        m->tables[join->s.table_id].list = result_S;
        delete_s = 0;
      }

      //B.5 Now we update the rest of the concatenated lists in the middleman
      for(int k = 0; k < q->number_of_predicates; k++)
      {
        if(concatenated_tables[k][0] == 0)
          continue;

        //Swipe row - if flag = 1 or flag = 2 then we found something in this row
        int flag = 0;
        for(int l = 1; l <= concatenated_tables[k][0]; l++)
        {
          if(concatenated_tables[k][l] == join->r.table_id)
          {
            flag = 1;
            break;
          }
          if(concatenated_tables[k][l] == join->s.table_id)
          {
            flag = 2;
            break;
          }
        }

        //If r or s exists then update
        if(flag){

          for(int l = 1; l <= concatenated_tables[k][0]; l++)
          {
            //Update all relations except for r or s
            if(concatenated_tables[k][l] == join->r.table_id || concatenated_tables[k][l] == join->s.table_id)
              continue;

            middle_list *new_list = create_middle_list();

            //Create lookup table for the old list
            middle_list_bucket **lookup = construct_lookup_table(m->tables[concatenated_tables[k][l]].list);

            middle_list_node *list_temp;

            //Select which list to traverse
            if(q->predicates[i].type == Join)
            {
              if(flag == 1)
                list_temp = result_R->head;
              else
                list_temp = result_S->head;
            }
            else
              list_temp = index_list->head;

            while(list_temp != NULL)
            {
              if(update_middle_bucket(lookup, &(list_temp->bucket), new_list))
              {
                fprintf(stderr, "execute_query: Error in update_middle_bucket\n");
                return NULL;
              }
              list_temp = list_temp->next;
            }

            delete_middle_list(m->tables[concatenated_tables[k][l]].list);
            m->tables[concatenated_tables[k][l]].list = new_list;

            free(lookup);
          }
        }
      }

      //B.6 Now we update the concatenated_tables
      //B.6.1 Find place of r and s
      int r_position = -1, s_position = -1;
      for(int k = 0; k < q->number_of_predicates; k++)
      {
        if(concatenated_tables[k][0] == 0)
          continue;

        //Swipe row and try to locate the row of r and s
        for(int l = 1; l <= concatenated_tables[k][0]; l++)
        {
          if(concatenated_tables[k][l] == join->r.table_id)
            r_position = k;
          else if(concatenated_tables[k][l] == join->s.table_id)
            s_position = k;
        }
      }


      //B.6.2 Update table
      if(r_position == -1 && s_position == -1)//Neither r nor s exist..add them
      {
        for(int m = 0; m < q->number_of_predicates; m++)
        {
          if(concatenated_tables[m][0] == 0)
          {
            concatenated_tables[m][1] = join->r.table_id;
            concatenated_tables[m][2] = join->s.table_id;
            concatenated_tables[m][0] = 2;
            break;
          }
        }
      }
      else if(r_position == -1 && s_position != -1)//s exists while r does not..add r in the row of s
      {
        concatenated_tables[s_position][0]++;
        concatenated_tables[s_position][concatenated_tables[s_position][0]] = join->r.table_id;
      }
      else if(r_position != -1 && s_position == -1)//r exists while d does not..add s in the row of r
      {
        concatenated_tables[r_position][0]++;
        concatenated_tables[r_position][concatenated_tables[r_position][0]] = join->s.table_id;
      }
      else if(r_position != s_position)//r and s are in different rows..merge them
      {
        int length = concatenated_tables[s_position][0];
        for(int m = 1; m <= length; m++)
        {
          concatenated_tables[r_position][0]++;

          concatenated_tables[r_position][concatenated_tables[r_position][0]] = concatenated_tables[s_position][m];

          concatenated_tables[s_position][0]--;

        }
      }

      //Delete result_R and result_S if necessary
      if(delete_r)
        delete_middle_list(result_R);

      if(delete_s)
        delete_middle_list(result_S);

      if(index_list != NULL)
        delete_middle_list(index_list);

    }
    //Case C. Original Self-Join
    else if(q->predicates[i].type == Self_Join &&
        (((predicate_join *)q->predicates[i].p)->r.table_id == ((predicate_join *)q->predicates[i].p)->s.table_id))
    {
      predicate_join *join = q->predicates[i].p;

      //find original tables
      table *table = get_table(index, q->table_ids[join->r.table_id]);
      if(table == NULL)
      {
        fprintf(stderr, "execute_query: Table not found\n");
        return NULL;
      }

      if(m->tables[join->r.table_id].list == NULL)
      {
        m->tables[join->r.table_id].list = create_middle_list();

        if(self_join_table(join, table, m->tables[join->r.table_id].list))
        {
          fprintf(stderr, "execute_query: Error in self_join_table\n");
          return NULL;
        }
      }
      else
      {
        middle_list *new_list = create_middle_list();
        middle_list_node *list_temp = m->tables[join->r.table_id].list->head;
        while(list_temp != NULL)
        {
          if(original_self_join_middle_bucket(join, &(list_temp->bucket), table, new_list))
          {
            fprintf(stderr, "execute_query: Error in original_self_join_middle_bucket\n");
            return NULL;
          }
          list_temp = list_temp->next;
        }

        delete_middle_list(m->tables[join->r.table_id].list);
        m->tables[join->r.table_id].list = new_list;
      }
    }
    else
    {
      fprintf(stderr, "execute_query: Undefined predicate type\n");
      return NULL;
    }
  }

  return m;
}




void calculate_sum(projection p, middle_list_bucket *bucket, table *table, uint64_t *sum)
{
  for(unsigned int i = 0; i < bucket->index_to_add_next; i++)
  {
    *sum += table->array[p.column_to_project.column_id][bucket->row_ids[i]];
  }
}


void calculate_projections(query *q, table_index* index, middleman *m)
{
  for(uint32_t i = 0; i < q->number_of_projections; i++)
  {
    projection p = q->projections[i];

    if(m->tables[p.column_to_project.table_id].list==NULL||m->tables[p.column_to_project.table_id].list->number_of_nodes==0)
    {
      fprintf(stderr, "\e[1;33mNULL \e[0m");
      continue;
    }

    table *original_table = get_table(index, q->table_ids[p.column_to_project.table_id]);
    if(original_table == NULL)
    {
      fprintf(stderr, "calculate_projections: Table not found\n");
      return;
    }

    uint64_t sum = 0;
    middle_list_node *list_temp = m->tables[p.column_to_project.table_id].list->head;
    while(list_temp != NULL)
    {
      calculate_sum(p, &(list_temp->bucket), original_table, &sum);
      list_temp = list_temp->next;
    }

    fprintf(stderr, "\e[1;33m%" PRIu64 " \e[0m", sum);
  }
  fprintf(stderr, "\n");

}


