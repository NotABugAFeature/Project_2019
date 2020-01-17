#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include <stdbool.h>
#include "execute_query.h"
#include "sort_merge_join.h"
#include "query.h"
#include "middle_list.h"
#include "table.h"
/**
 * Middleman initializator & memory allocator
 *
 * @param receives a number of tables
 * @return a middleman struct with 'number_of_tables' elements
 */
middleman *initialize_middleman(uint32_t number_of_tables)
{
    if(number_of_tables<1)
    {
        fprintf(stderr, "initialize_middleman: Cannot create middleman with %d tables\n", number_of_tables);
        return NULL;
    }
    middleman *m=malloc(sizeof(middleman));
    if(m==NULL)
    {
        fprintf(stderr, "Cannot allocate memory for middleman\n");
        return NULL;
    }
    m->number_of_tables=number_of_tables;
    m->tables=malloc(number_of_tables*sizeof(middle_table));
    if(m->tables==NULL)
    {
        free(m);
        fprintf(stderr, "Cannot allocate memory for middle_tables\n");
        return NULL;
    }
    for(uint32_t i=0; i<m->number_of_tables; i++)
    {
        m->tables[i].list=NULL;
    }
    return m;
}
/**
 * Traverses the elements of a bucket and filters them according to a value
 * The results are stored in a new list (new_list)
 *
 * @return 1 in case of error or 0 if successful
 */
int filter_middle_bucket(predicate_filter *filter, middle_list_bucket *bucket, table* table, middle_list *new_list)
{
    if(filter==NULL||table==NULL||table->array==NULL||new_list==NULL||bucket==NULL)
    {
        fprintf(stderr, "filter_middle_bucket: Error with parameters\n");
        return 1;
    }
    if(filter->filter_type==Less)
    {
        for(uint32_t i=0; i<bucket->index_to_add_next; i++)
        {
            if(table->array[filter->r.column_id][bucket->row_ids[i]]<filter->value)
            {
                if(append_to_middle_list(new_list, bucket->row_ids[i]))
                {
                    return 1;
                }
            }
        }
    }
    else if(filter->filter_type==Less_Equal)
    {
        for(uint32_t i=0; i<bucket->index_to_add_next; i++)
        {
            if(table->array[filter->r.column_id][bucket->row_ids[i]]<=filter->value)
            {
                if(append_to_middle_list(new_list, bucket->row_ids[i]))
                {
                    return 1;
                }
            }
        }
    }
    else if(filter->filter_type==Equal)
    {
        for(uint32_t i=0; i<bucket->index_to_add_next; i++)
        {
            if(table->array[filter->r.column_id][bucket->row_ids[i]]==filter->value)
            {
                if(append_to_middle_list(new_list, bucket->row_ids[i]))
                {
                    return 1;
                }
            }
        }
    }
    else if(filter->filter_type==Greater)
    {
        for(uint32_t i=0; i<bucket->index_to_add_next; i++)
        {
            if(table->array[filter->r.column_id][bucket->row_ids[i]]>filter->value)
            {
                if(append_to_middle_list(new_list, bucket->row_ids[i]))
                {
                    return 1;
                }
            }
        }
    }
    else if(filter->filter_type==Greater_Equal)
    {
        for(uint32_t i=0; i<bucket->index_to_add_next; i++)
        {
            if(table->array[filter->r.column_id][bucket->row_ids[i]]>=filter->value)
            {
                if(append_to_middle_list(new_list, bucket->row_ids[i]))
                {
                    return 1;
                }
            }
        }
    }
    else if(filter->filter_type==Not_Equal)
    {
        for(uint32_t i=0; i<bucket->index_to_add_next; i++)
        {
            if(table->array[filter->r.column_id][bucket->row_ids[i]]!=filter->value)
            {
                if(append_to_middle_list(new_list, bucket->row_ids[i]))
                {
                    return 1;
                }
            }
        }
    }
    else
    {
        return -1;
    }
    return 0;
}
int original_self_join_middle_bucket(predicate_join *join, middle_list_bucket *bucket, table* table, middle_list *new_list)
{
    if(join==NULL||table==NULL||table->array==NULL||new_list==NULL||bucket==NULL)
    {
        fprintf(stderr, "original_self_join_middle_bucket: Error with parameters\n");
        return 1;
    }
    for(unsigned int i=0; i<bucket->index_to_add_next; i++)
    {
        if(table->array[join->r.column_id][bucket->row_ids[i]]==table->array[join->s.column_id][bucket->row_ids[i]])
        {
            if(append_to_middle_list(new_list, bucket->row_ids[i]))
            {
                return 1;
            }
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
    if(table==NULL||table->array==NULL||column_id<0||column_id>=table->columns)
    {
        fprintf(stderr, "construct_relation_from_table: Incorrect arguments\n");
        return NULL;
    }
    relation *rel=malloc(sizeof(relation));
    if(rel==NULL)
    {
        fprintf(stderr, "construct_relation_from_table: Cannot allocate memory\n");
        return NULL;
    }
    rel->num_tuples=table->rows;
    rel->tuples=malloc((rel->num_tuples)*sizeof(tuple));
    if(rel==NULL)
    {
        fprintf(stderr, "construct_relation_from_table: Cannot allocate memory\n");
        return NULL;
    }
    for(uint64_t i=0; i<rel->num_tuples; i++)
    {
        rel->tuples[i].key=table->array[column_id][i];
        rel->tuples[i].row_id=i;
    }
    return rel;
}
/**
 * Constructs a 'relation' for the join operation based on the elements of a bucket
 * The results are stored to a relation (rel)
 */
void construct_relation_from_middleman(middle_list_bucket *bucket, table *table, relation *rel, uint64_t column_id, uint64_t *counter)
{
    for(uint32_t i=0; i<bucket->index_to_add_next; i++)
    {
        rel->tuples[(*counter)].key=table->array[column_id][bucket->row_ids[i]];
        rel->tuples[(*counter)].row_id= *counter;
        (*counter)++;
    }
}
int update_related_lists(uint32_t predicate_index, query *q, uint32_t **concatenated_tables,
                         middleman *m, int delete_r, int delete_s,
                         middle_list *result_R,
                         middle_list *result_S, middle_list *index_list)
{
    if(q==NULL||q->number_of_predicates<=predicate_index||concatenated_tables==NULL||
       m==NULL||((result_R==NULL||result_S==NULL)&&q->predicates[predicate_index].type==Join)||
       (index_list==NULL&&q->predicates[predicate_index].type==Self_Join))
    {
        fprintf(stderr, "update_related_lists: Error with parameters\n");
        return -1;
    }
    predicate_join *join=(predicate_join*) q->predicates[predicate_index].p;
    //B-a.5 Now we update the rest of the concatenated lists in the middleman
    for(int k=0; k<q->number_of_predicates; k++)
    {
        if(concatenated_tables[k][0]==0)
        {
            continue;
        }
        //Swipe row - if flag = 1 or flag = 2 then we found something in this row
        int flag=0;
        for(int l=1; l<=concatenated_tables[k][0]; l++)
        {
            if(concatenated_tables[k][l]==join->r.table_id)
            {
                flag=1;
                break;
            }
            if(concatenated_tables[k][l]==join->s.table_id)
            {
                flag=2;
                break;
            }
        }
        //If r or s exists then update
        if(flag)
        {
            for(int l=1; l<=concatenated_tables[k][0]; l++)
            {
                //Update all relations except for r or s
                if(concatenated_tables[k][l]==join->r.table_id||concatenated_tables[k][l]==join->s.table_id)
                {
                    continue;
                }
                middle_list *new_list=create_middle_list();
                //Create lookup table for the old list
#if defined(SERIAL_EXECUTION)||(defined(SERIAL_JOIN)&&defined(SERIAL_FILTER)&&defined(SERIAL_SELFJOIN))
                middle_list_bucket **lookup=construct_lookup_table(m->tables[concatenated_tables[k][l]].list);
#else
                lookup_table *lookup=construct_lookup_table(m->tables[concatenated_tables[k][l]].list);
#endif
                middle_list_node *list_temp;
                //Select which list to traverse
                if(q->predicates[predicate_index].type==Join)
                {
                    if(flag==1)
                    {
                        list_temp=result_R->head;
                    }
                    else
                    {
                        list_temp=result_S->head;
                    }
                }
                else
                {
                    list_temp=index_list->head;
                }
                while(list_temp!=NULL)
                {
                    if(update_middle_bucket(lookup, &(list_temp->bucket), new_list))
                    {
                        fprintf(stderr, "execute_query: Error in update_middle_bucket\n");
                        return 1;
                    }
                    list_temp=list_temp->next;
                }
                delete_middle_list(m->tables[concatenated_tables[k][l]].list);
                m->tables[concatenated_tables[k][l]].list=new_list;
#if defined(SERIAL_EXECUTION)||(defined(SERIAL_JOIN)&&defined(SERIAL_FILTER)&&defined(SERIAL_SELFJOIN))
                free(lookup);
#else
                delete_lookup_table(lookup);
#endif
            }
        }
    }
    //B-a.6 Now we update the concatenated_tables
    //B-a.6.1 Find place of r and s
    int r_position= -1, s_position= -1;
    for(int k=0; k<q->number_of_predicates; k++)
    {
        if(concatenated_tables[k][0]==0)
        {
            continue;
        }
        //Swipe row and try to locate the row of r and s
        for(int l=1; l<=concatenated_tables[k][0]; l++)
        {
            if(concatenated_tables[k][l]==join->r.table_id)
            {
                r_position=k;
            }
            else if(concatenated_tables[k][l]==join->s.table_id)
            {
                s_position=k;
            }
        }
    }
    //B-a.6.2 Update table
    if(r_position== -1&&s_position== -1)//Neither r nor s exist..add them
    {
        for(int m=0; m<q->number_of_predicates; m++)
        {
            if(concatenated_tables[m][0]==0)
            {
                concatenated_tables[m][1]=join->r.table_id;
                concatenated_tables[m][2]=join->s.table_id;
                concatenated_tables[m][0]=2;
                break;
            }
        }
    }
    else if(r_position== -1&&s_position!= -1)//s exists while r does not..add r in the row of s
    {
        concatenated_tables[s_position][0]++;
        concatenated_tables[s_position][concatenated_tables[s_position][0]]=join->r.table_id;
    }
    else if(r_position!= -1&&s_position== -1)//r exists while d does not..add s in the row of r
    {
        concatenated_tables[r_position][0]++;
        concatenated_tables[r_position][concatenated_tables[r_position][0]]=join->s.table_id;
    }
    else if(r_position!=s_position)//r and s are in different rows..merge them
    {
        int length=concatenated_tables[s_position][0];
        for(int m=1; m<=length; m++)
        {
            concatenated_tables[r_position][0]++;
            concatenated_tables[r_position][concatenated_tables[r_position][0]]=concatenated_tables[s_position][m];
            concatenated_tables[s_position][0]--;
        }
    }
    //Delete result_R and result_S if necessary
    if(delete_r)
        delete_middle_list(result_R);
    if(delete_s)
        delete_middle_list(result_S);
    if(index_list!=NULL)
        delete_middle_list(index_list);
    return 0;
}
#if defined(SERIAL_EXECUTION)||(defined(SERIAL_JOIN)&&defined(SERIAL_FILTER)&&defined(SERIAL_SELFJOIN))
/**
 * After join, we go back to the middleman and for every rowId of the result 
 * we find the equivalent content in the middleman.
 * The result is stored in a new list (updated_list)
 * 
 * @return 1 in case of error or 0 if successful
 */
int update_middle_bucket(middle_list_bucket **lookup, middle_list_bucket *bucket, middle_list *updated_list)
{
    for(uint32_t i=0; i<bucket->index_to_add_next; i++)
    {
        middle_list_bucket *target=lookup[(bucket->row_ids[i])/middle_LIST_BUCKET_SIZE];
        if(target==NULL)
        {
            fprintf(stderr, "update_middle_bucket: Target bucket not found in lookup table\n");
            return 2;
        }
        if(append_to_middle_list(updated_list, target->row_ids[(bucket->row_ids[i])%middle_LIST_BUCKET_SIZE]))
            return 1;
    }
    return 0;
}
#else
/**
 * After join, we go back to the middleman and for every rowId of the result 
 * we find the equivalent content in the middleman.
 * The result is stored in a new list (updated_list)
 * 
 * @return 1 in case of error or 0 if successful
 */
int update_middle_bucket(lookup_table*lookup, middle_list_bucket *bucket, middle_list *updated_list)
{
    middle_list_bucket *target=NULL;
    uint64_t lookup_index=0;
    for(uint32_t i=0; i<bucket->index_to_add_next; i++)
    {
        lookup_index=(bucket->row_ids[i])/middle_LIST_BUCKET_SIZE;
        while(true)
        {
            if(lookup_index>=lookup->size)//Not needed only for debugging
            {
                fprintf(stderr, "update_middle_bucket: Target bucket not found in lookup table\n");
                return 2;
            }
            if(lookup->min[lookup_index]>bucket->row_ids[i])
            {
                lookup_index--;
            }
            else if(lookup->max[lookup_index]<bucket->row_ids[i])
            {
                lookup_index++;
            }
            else
            {
                break;
            }
        }
        target=lookup->lookup_table[lookup_index];
        if(append_to_middle_list(updated_list, target->row_ids[bucket->row_ids[i]-lookup->min[lookup_index]]))
        {
            return 1;
        }
    }
    return 0;
}
#endif
/**
 * Indirect Self-join of two buckets in the middleman
 * The rowIds of equal columns are stored in two new lists(list_r, list_s)
 * 
 * @return 1 in case of error or 0 if successful
 */
int self_join_middle_bucket(predicate_join *join, table *table_r, table *table_s, middle_list_bucket *bucket_r,
                            middle_list_bucket *bucket_s, middle_list *list_r, middle_list *list_s, middle_list *index_list,
                            uint32_t *counter)
{
    if(join==NULL||table_r==NULL||table_s==NULL||table_r->array==NULL||table_s->array==NULL||
       bucket_r==NULL||bucket_s==NULL||list_r==NULL||list_s==NULL||index_list==NULL||counter==NULL)
    {
        fprintf(stderr, "self_join_middle_bucket: Error with parameters\n");
        return 1;
    }
    for(uint32_t i=0; i<bucket_r->index_to_add_next; i++)
    {
        if(table_r->array[join->r.column_id][bucket_r->row_ids[i]]==
           table_s->array[join->s.column_id][bucket_s->row_ids[i]])
        {
            if(append_to_middle_list(list_r, bucket_r->row_ids[i])||
               append_to_middle_list(list_s, bucket_s->row_ids[i])||
               append_to_middle_list(index_list, *counter))
            {
                return 1;
            }
        }
        (*counter)++;
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
    if(join==NULL||table==NULL||table->array==NULL||list==NULL)
    {
        fprintf(stderr, "self_join_table: Error with parameters\n");
        return 1;
    }
    for(uint64_t i=0; i<table->rows; i++)
    {
        if(table->array[join->r.column_id][i]==table->array[join->s.column_id][i])
        {
            if(append_to_middle_list(list, i))
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
    if(filter==NULL||table==NULL||table->array==NULL||new_list==NULL)
    {
        fprintf(stderr, "filter_original_table: Error with parameters\n");
        return 1;
    }
    if(filter->filter_type==Less)
    {
        for(uint64_t i=0; i<table->rows; i++)
        {
            if(table->array[filter->r.column_id][i]<filter->value)
            {
                if(append_to_middle_list(new_list, i))
                {
                    return 1;
                }
            }
        }
    }
    else if(filter->filter_type==Less_Equal)
    {
        for(uint64_t i=0; i<table->rows; i++)
        {
            if(table->array[filter->r.column_id][i]<=filter->value)
            {
                if(append_to_middle_list(new_list, i))
                {
                    return 1;
                }
            }
        }
    }
    else if(filter->filter_type==Equal)
    {
        for(uint64_t i=0; i<table->rows; i++)
        {
            if(table->array[filter->r.column_id][i]==filter->value)
            {
                if(append_to_middle_list(new_list, i))
                {
                    return 1;
                }
            }
        }
    }
    else if(filter->filter_type==Greater)
    {
        for(uint64_t i=0; i<table->rows; i++)
        {
            if(table->array[filter->r.column_id][i]>filter->value)
            {
                if(append_to_middle_list(new_list, i))
                {
                    return 1;
                }
            }
        }
    }
    else if(filter->filter_type==Greater_Equal)
    {
        for(uint64_t i=0; i<table->rows; i++)
        {
            if(table->array[filter->r.column_id][i]>=filter->value)
            {
                if(append_to_middle_list(new_list, i))
                {
                    return 1;
                }
            }
        }
    }
    else if(filter->filter_type==Not_Equal)
    {
        for(uint64_t i=0; i<table->rows; i++)
        {
            if(table->array[filter->r.column_id][i]!=filter->value)
            {
                if(append_to_middle_list(new_list, i))
                {
                    return 1;
                }
            }
        }
    }
    else
    {
        return -1;
    }
    return 0;
}
/**
 * Indirect Self-join of two buckets in the middleman
 * The rowIds of equal columns are stored in two new lists(list_r, list_s)
 * 
 * @return 1 in case of error or 0 if successful
 */
int self_join_middle_bucket_parallel(predicate_join *join, table *table_r, table *table_s, middle_list_bucket *bucket_r,
                                     middle_list_bucket *bucket_s, middle_list *list_r, middle_list *list_s, middle_list *index_list,
                                     uint32_t *r_index, uint32_t *s_index, uint32_t *counter)
{
    if(join==NULL||table_r==NULL||table_s==NULL||table_r->array==NULL||table_s->array==NULL||
       bucket_r==NULL||bucket_s==NULL||list_r==NULL||list_s==NULL||index_list==NULL||counter==NULL)
    {
        fprintf(stderr, "self_join_middle_bucket: Error with parameters\n");
        return 1;
    }
    //  for(uint32_t i = 0; i < bucket_r->index_to_add_next; i++)
    for(; *r_index<bucket_r->index_to_add_next&&*s_index<bucket_s->index_to_add_next; (*r_index)++, (*s_index)++)
    {
        if(table_r->array[join->r.column_id][bucket_r->row_ids[*r_index]]==
           table_s->array[join->s.column_id][bucket_s->row_ids[*s_index]])
        {
            if(append_to_middle_list(list_r, bucket_r->row_ids[*r_index])||
               append_to_middle_list(list_s, bucket_s->row_ids[*s_index])||
               append_to_middle_list(index_list, *counter))
            {
                return 1;
            }
        }
        (*counter)++;
    }
    return 0;
}
/**
 * Traverses the elements of a table and filters them according to a value
 * The results are stored in a new list (new_list)
 *
 * @return 1 in case of error or 0 if successful
 */
int filter_original_table_parallel(predicate_filter *filter, table* table, uint64_t start_index, uint64_t end_index, middle_list *new_list)
{
    if(filter==NULL||table==NULL||table->array==NULL||new_list==NULL||start_index>end_index||table->rows<end_index)
    {
        fprintf(stderr, "filter_original_table: Error with parameters\n");
        return 1;
    }
    if(filter->filter_type==Less)
    {
        for(uint64_t i=start_index; i<end_index; i++)
        {
            if(table->array[filter->r.column_id][i]<filter->value)
            {
                if(append_to_middle_list(new_list, i))
                {
                    return 1;
                }
            }
        }
    }
    else if(filter->filter_type==Less_Equal)
    {
        for(uint64_t i=start_index; i<end_index; i++)
        {
            if(table->array[filter->r.column_id][i]<=filter->value)
            {
                if(append_to_middle_list(new_list, i))
                {
                    return 1;
                }
            }
        }
    }
    else if(filter->filter_type==Equal)
    {
        for(uint64_t i=start_index; i<end_index; i++)
        {
            if(table->array[filter->r.column_id][i]==filter->value)
            {
                if(append_to_middle_list(new_list, i))
                {
                    return 1;
                }
            }
        }
    }
    else if(filter->filter_type==Greater)
    {
        for(uint64_t i=start_index; i<end_index; i++)
        {
            if(table->array[filter->r.column_id][i]>filter->value)
            {
                if(append_to_middle_list(new_list, i))
                {
                    return 1;
                }
            }
        }
    }
    else if(filter->filter_type==Greater_Equal)
    {
        for(uint64_t i=start_index; i<end_index; i++)
        {
            if(table->array[filter->r.column_id][i]>=filter->value)
            {
                if(append_to_middle_list(new_list, i))
                {
                    return 1;
                }
            }
        }
    }
    else if(filter->filter_type==Not_Equal)
    {
        for(uint64_t i=start_index; i<end_index; i++)
        {
            if(table->array[filter->r.column_id][i]!=filter->value)
            {
                if(append_to_middle_list(new_list, i))
                {
                    return 1;
                }
            }
        }
    }
    else
    {
        return -1;
    }
    return 0;
}
/**
 * Takes a table and performs a self-join based on two columns 
 * The rowIds of equal columns are stored in a list (list)
 * 
 * @return 1 in case of error or 0 if successful
 */
int self_join_table_parallel(predicate_join *join, table* table, uint64_t start, uint64_t end, middle_list *list)
{
    if(join==NULL||table==NULL||table->array==NULL||list==NULL||start>end)
    {
        fprintf(stderr, "self_join_table: Error with parameters\n");
        return 1;
    }
    for(uint64_t i=start; i<end; i++)
    {
        if(table->array[join->r.column_id][i]==table->array[join->s.column_id][i])
        {
            if(append_to_middle_list(list, i))
                return 1;
        }
    }
    return 0;
}
int execute_query_parallel(job_query_parameters* p)
{
    //Check for proper argument format
    if(p->query==NULL||p->tables==NULL||p->bool_array==NULL)
    {
        fprintf(stderr, "execute_query_parallel: Null parameters\n");
        return -1;
    }
    //Check if this is the first time executing the query
    if(p->b_index==0&&p->pred_index==0&&p->middle==NULL&&p->joined_tables==NULL)
    {
        //First time
        //Array that keeps which table have become one
        p->joined_tables=malloc(sizeof(uint32_t*)*p->query->number_of_predicates);
        if(p->joined_tables==NULL)
        {
            perror("execute_query_parallel: malloc error");
            return -1;
        }
        for(uint32_t i=0; i<p->query->number_of_predicates; i++)
        {
            p->joined_tables[i]=malloc(sizeof(uint32_t)*(p->query->number_of_tables+1));
            if(p->joined_tables[i]==NULL)
            {
                perror("execute_query_parallel: malloc error");
                for(uint32_t j=0; j<i; j++)
                {
                    free(p->joined_tables[i]);
                }
                free(p->joined_tables);
                p->joined_tables=NULL;
                return -1;
            }
        }
        for(uint32_t i=0; i<p->query->number_of_predicates; i++)
        {
            for(uint32_t j=0; j<p->query->number_of_tables+1; j++)
            {
                p->joined_tables[i][j]=0;
            }
        }
        //Initialize middleman
        p->middle=initialize_middleman(p->query->number_of_tables);
        if(p->middle==NULL)
        {
            return -2;
        }
    }
    else if(p->pred_index!=0&&p->middle!=NULL&&p->joined_tables!=NULL)
    {//Nothing to do
    }
    else
    {
        fprintf(stderr, "execute_query_parallel: parameters not valid\n");
        return -3;
    }
    //Execute every predicate sequentially
    while(p->pred_index<p->query->number_of_predicates)
    {
        for(uint32_t i=0; i<p->middle->number_of_tables; i++)
        { //Check if a result is NULL and we can stop
            if(p->middle->tables[i].list!=NULL&&p->middle->tables[i].list->number_of_records==0)
            {
                break;
            }
        }
        //Case A. filter
        if(p->query->predicates[p->pred_index].type==Filter)
        {
            predicate_filter *filter=p->query->predicates[p->pred_index].p;
            //A.1 Find original table
            table *original_table=get_table(p->tables, p->query->table_ids[filter->r.table_id]);
            if(original_table==NULL)
            {
                fprintf(stderr, "execute_query: Table not found\n");
                return -4;
            }
#if defined(SERIAL_EXECUTION)||defined(SERIAL_FILTER)
            //A.2 Check if there is a middleman list
            if(p->middle->tables[filter->r.table_id].list==NULL)
            {
                //A.2.1 If it is empty we need to filter the data of the original table..
                p->middle->tables[filter->r.table_id].list=create_middle_list();
                if(filter_original_table(filter, original_table, p->middle->tables[filter->r.table_id].list))
                {
                    fprintf(stderr, "execute_query filter_original_table: Error\n");
                    return -4;
                }
            }
            else
            {
                //A.2.2 ..else we filter the data of the middleman's list
                if(p->middle->tables[filter->r.table_id].list->number_of_nodes>0)
                {
                    middle_list *new_list=create_middle_list();
                    //traverse existing list and store the new results in 'new_list'
                    middle_list_node *list_temp=p->middle->tables[filter->r.table_id].list->head;
                    while(list_temp!=NULL)
                    {
                        if(filter_middle_bucket(filter, &(list_temp->bucket), original_table, new_list))
                        {
                            fprintf(stderr, "execute_query filter_middle_bucket: Error\n");
                            return -4;
                        }
                        list_temp=list_temp->next;
                    }
                    //delete old list and put the new one in its place
                    delete_middle_list(p->middle->tables[filter->r.table_id].list);
                    p->middle->tables[filter->r.table_id].list=new_list;
                }
            }
#else
            //A.2 Check if there is a middleman list
            if(p->middle->tables[filter->r.table_id].list==NULL)
            {
                //A.2.1 If it is empty we need to filter the data of the original table..
                //TODO Figure out size of parts
                uint64_t parts;
                if(original_table->rows<FILTER_TUPLES)
                {
                    parts=1;
                }
                else
                {
                    parts=original_table->rows/FILTER_TUPLES+1;
                }
                uint64_t small_size=original_table->rows/parts;
                uint64_t extra=original_table->rows%parts;
                p->r_counter=parts;
                //Create a list_array to keep the results of each part
                list_array *la=create_list_array(parts, 1);
                if(la==NULL)
                {
                    fprintf(stderr, "execute_query: error in create_list_array\n");
                    return -1;
                }
                uint64_t start_index, end_index=0;
                for(uint64_t i=0; i<parts; i++)
                {
                    start_index=end_index;
                    end_index=start_index+small_size;
                    if(i<extra)
                    {
                        end_index++;
                    }
                    //Create and schedule the filter job
                    job *newjob=create_filter_table_job(original_table, start_index, end_index, &p->r_counter, &p->r_mutex, la, i, p);
                    schedule_fast_job(p->this_job->scheduler, newjob);
                }
                return 0;
            }
            else
            {
                //A.2.2 ..else we filter the data of the middleman's list
                //TODO Figure out size of parts
                uint64_t parts;
                if(p->middle->tables[filter->r.table_id].list->number_of_nodes<FILTER_MIDDLE_BUCKETS)
                {
                    parts=1;
                }
                else
                {
                    parts=p->middle->tables[filter->r.table_id].list->number_of_nodes/FILTER_MIDDLE_BUCKETS+1;
                }
                uint64_t small_size=p->middle->tables[filter->r.table_id].list->number_of_nodes/parts;
                uint64_t extra=p->middle->tables[filter->r.table_id].list->number_of_nodes%parts;
                p->r_counter=parts;
                //Create a list_array to keep the results of each part
                list_array *la=create_list_array(parts, 1);
                if(la==NULL)
                {
                    fprintf(stderr, "execute_query: error in create_list_array\n");
                    return -1;
                }
                middle_list_node* job_node=p->middle->tables[filter->r.table_id].list->head;
                middle_list_node* temp_node=NULL;
                uint64_t node_count=0;
                for(uint64_t i=0; i<parts&&job_node!=NULL; i++)
                {
                    node_count=small_size;
                    if(i<extra)
                    {
                        node_count++;
                    }
                    //Create and schedule the filter job
                    job *newjob=create_filter_middle_job(original_table, job_node, node_count, &p->r_counter, &p->r_mutex, la, i, p);
                    schedule_fast_job(p->this_job->scheduler, newjob);
                    temp_node=job_node;
                    while(node_count>0)
                    {
                        temp_node=temp_node->next;
                        node_count--;
                    }
                    job_node=temp_node;
                }
                return 0;
            }
#endif
        }
        //Case B. Original Join or Indirect Self-Join
        else if(p->query->predicates[p->pred_index].type==Join)
        {
#if defined(SERIAL_EXECUTION)||(defined(SERIAL_JOIN)&&defined(SERIAL_PRESORTING)&&defined(SERIAL_SORTING))
            predicate_join *join=p->query->predicates[p->pred_index].p;
            middle_list *index_list=NULL;
            int delete_r=1, delete_s=1;
            //B-a.1 Create middle lists in which we are going to store the join results
            middle_list *result_R=create_middle_list();
            if(result_R==NULL)
            {
                fprintf(stderr, "execute_query: Error in create_result_list\n");
                return -5;
            }
            middle_list *result_S=create_middle_list();
            if(result_S==NULL)
            {
                fprintf(stderr, "execute_query: Error in create_result_list\n");
                return -5;
            }
            //B-a.2 Find original tables
            table *table_r=get_table(p->tables, p->query->table_ids[join->r.table_id]);
            if(table_r==NULL)
            {
                fprintf(stderr, "execute_query:  Null parameters\n");
                return -6;
            }
            table *table_s=get_table(p->tables, p->query->table_ids[join->s.table_id]);
            if(table_s==NULL)
            {
                fprintf(stderr, "execute_query:  Null parameters\n");
                return -7;
            }
            //B-a.3.1 Construct relation relR
            //Check if table exists in middleman
            //If yes use it else, if not take if from the original table
            relation *relR;
            if(p->middle->tables[join->r.table_id].list==NULL)
            {
                relR=construct_relation_from_table(table_r, join->r.column_id);
            }
            else
            {
                //relR memory allocation
                relR=malloc(sizeof(relation));
                if(relR==NULL)
                {
                    fprintf(stderr, "execute_query: Cannot allocate memory\n");
                    return -8;
                }
                relR->num_tuples=middle_list_get_number_of_records(p->middle->tables[join->r.table_id].list);
                if(relR->num_tuples>0)
                {
                    relR->tuples=malloc((relR->num_tuples)*sizeof(tuple));
                    if(relR->tuples==NULL)
                    {
                        fprintf(stderr, "execute_query: Cannot allocate memory\n");
                        return -9;
                    }
                    //Construct relation from middleman
                    uint64_t counter=0;
                    middle_list_node *list_temp=p->middle->tables[join->r.table_id].list->head;
                    while(list_temp!=NULL)
                    {
                        construct_relation_from_middleman(&(list_temp->bucket), table_r, relR, join->r.column_id, &counter);
                        list_temp=list_temp->next;
                    }
                }
            }
            //B-a.3.2 Construct relation relS
            //Check if table exists in middleman
            //If yes use it else, if not take if from the original table
            relation *relS;
            if(p->middle->tables[join->s.table_id].list==NULL)
            {
                relS=construct_relation_from_table(table_s, join->s.column_id);
            }
            else
            {
                relS=malloc(sizeof(relation));
                if(relS==NULL)
                {
                    fprintf(stderr, "execute_query: Cannot allocate memory\n");
                    return -10;
                }
                relS->num_tuples=middle_list_get_number_of_records(p->middle->tables[join->s.table_id].list);
                if(relS->num_tuples>0)
                {
                    relS->tuples=malloc((relS->num_tuples)*sizeof(tuple));
                    if(relS->tuples==NULL)
                    {
                        fprintf(stderr, "execute_query: Cannot allocate memory\n");
                        return -11;
                    }
                    uint64_t counter=0;
                    middle_list_node *list_temp=p->middle->tables[join->s.table_id].list->head;
                    while(list_temp!=NULL)
                    {
                        construct_relation_from_middleman(&(list_temp->bucket), table_s, relS, join->s.column_id, &counter);
                        list_temp=list_temp->next;
                    }
                }
            }
            //B-a.3.3 relR and RelS are ready..Sort if necessary
            if(p->bool_array[p->b_index])
            {
                if(radix_sort(relR))
                {
                    fprintf(stderr, "execute_query: Error in radix_sort\n");
                    return -12;
                }
            }
            if(p->bool_array[p->b_index+1])
            {
                if(radix_sort(relS))
                {
                    fprintf(stderr, "execute_query: Error in radix_sort\n");
                    return -13;
                }
            }
            //Update counter
            p->b_index+=2;
            //B-a.3.4 Join
            if(final_join(result_R, result_S, relR, relS))
            {
                fprintf(stderr, "execute_query: Error in final_join\n");
                return -14;
            }
            //B-a.3.5 Now go back to middleman
            //If the list exists then update it
            //If not then put the result lisr (result_R, result_S) in its place
            if(p->middle->tables[join->r.table_id].list==NULL)
            {
                p->middle->tables[join->r.table_id].list=result_R;
                delete_r=0;
            }
            else
            {
                middle_list *new_list=create_middle_list();
                if(result_R->number_of_nodes>0)
                {
                    //Construct lookup table of the existing (old) list
                    middle_list_bucket **lookup=construct_lookup_table(p->middle->tables[join->r.table_id].list);
                    //Traverse result list and for every rowId find it in the old list and put
                    //the result in the new_list
                    middle_list_node *list_temp=result_R->head;
                    while(list_temp!=NULL)
                    {
                        if(update_middle_bucket(lookup, &(list_temp->bucket), new_list))
                        {
                            fprintf(stderr, "execute_query: Error in update_middle_bucket\n");
                            return -15;
                        }
                        list_temp=list_temp->next;
                    }
                    free(lookup);
                }
                delete_middle_list(p->middle->tables[join->r.table_id].list);
                p->middle->tables[join->r.table_id].list=new_list;
            }
            //Same procedure for S as above
            if(p->middle->tables[join->s.table_id].list==NULL)
            {
                p->middle->tables[join->s.table_id].list=result_S;
                delete_s=0;
            }
            else
            {
                middle_list *new_list=create_middle_list();
                if(result_S->number_of_nodes>0)
                {
                    middle_list_bucket **lookup=construct_lookup_table(p->middle->tables[join->s.table_id].list);
                    middle_list_node *list_temp=result_S->head;
                    while(list_temp!=NULL)
                    {
                        if(update_middle_bucket(lookup, &(list_temp->bucket), new_list))
                        {
                            fprintf(stderr, "execute_query: Error in update_middle_bucket\n");
                            return -16;
                        }
                        list_temp=list_temp->next;
                    }
                    free(lookup);
                }
                delete_middle_list(p->middle->tables[join->s.table_id].list);
                p->middle->tables[join->s.table_id].list=new_list;
            }
            //B-a.3.6 Free relations
            if(relR->num_tuples>0)
            {
                free(relR->tuples);
            }
            free(relR);
            if(relS->num_tuples>0)
            {
                free(relS->tuples);
            }
            free(relS);
            update_related_lists(p->pred_index, p->query, p->joined_tables, p->middle, delete_r, delete_s, result_R, result_S, index_list);
#else
            predicate_join *join=p->query->predicates[p->pred_index].p;
            p->r_counter=1;
            p->s_counter=1;
            job* newjob_r=create_presort_job(p, &p->r, &join->r, &p->r_mutex, p->bool_array[p->b_index], &(p->r_counter));
            p->b_index++;
            if(newjob_r==NULL)
            {
                destroy_query_job((void*) p);
                return -2;
            }
            //Append to fifo
            //TODO Add checks
            schedule_fast_job(p->this_job->scheduler, newjob_r);
            job* newjob_s=create_presort_job(p, &p->s, &join->s, &p->s_mutex, p->bool_array[p->b_index], &(p->s_counter));
            p->b_index++;
            if(newjob_s==NULL)
            {
                destroy_query_job((void*) p);
                return -2;
            }
            //TODO Add checks
            schedule_fast_job(p->this_job->scheduler, newjob_s);
            return 0;
#endif
        }
        else if((p->query->predicates[p->pred_index].type==Self_Join&&
                 (((predicate_join *) p->query->predicates[p->pred_index].p)->r.table_id!=((predicate_join *) p->query->predicates[p->pred_index].p)->s.table_id)))
        {
            predicate_join *join=p->query->predicates[p->pred_index].p;
            middle_list *index_list=NULL;
            int delete_r=1, delete_s=1;
            //B.1 Create middle lists in which we are going to store the join results
            middle_list *result_R=create_middle_list();
            if(result_R==NULL)
            {
                fprintf(stderr, "execute_query: Error in create_result_list\n");
                return -3;
            }
            middle_list *result_S=create_middle_list();
            if(result_S==NULL)
            {
                fprintf(stderr, "execute_query: Error in create_result_list\n");
                return -3;
            }
            //B.2 Find original tables
            table *table_r=get_table(p->tables, p->query->table_ids[join->r.table_id]);
            if(table_r==NULL)
            {
                fprintf(stderr, "execute_query:  Null parameters\n");
                return -3;
            }
            table *table_s=get_table(p->tables, p->query->table_ids[join->s.table_id]);
            if(table_s==NULL)
            {
                fprintf(stderr, "execute_query:  Null parameters\n");
                return -3;
            }
            if(p->middle->tables[join->r.table_id].list==NULL||p->middle->tables[join->s.table_id].list==NULL)
            {
                fprintf(stderr, "execute_query: Indirect self-join failed\n");
                return -9;
            }
            index_list=create_middle_list();
            middle_list_node *list_temp_r=p->middle->tables[join->r.table_id].list->head;
            middle_list_node *list_temp_s=p->middle->tables[join->s.table_id].list->head;
            //Traverse the two lists simultaneously and keep only the rowIds 
            //with the same content in the original tables
#if defined(SERIAL_EXECUTION)||(defined(SERIAL_JOIN)&&defined(SERIAL_FILTER)&&defined(SERIAL_SELFJOIN))
            uint32_t counter=0;
            while(list_temp_r!=NULL)
            {
                if(self_join_middle_bucket(join, table_r, table_s, &(list_temp_r->bucket), &(list_temp_s->bucket), result_R, result_S, index_list, &counter))
                {
                    fprintf(stderr, "execute_query: Error in self_join_middle_bucket\n");
                    return -10;
                }
                list_temp_r=list_temp_r->next;
                list_temp_s=list_temp_s->next;
            }
#else
            uint32_t counter=0;
            uint32_t r_index=0;
            uint32_t s_index=0;
            while(list_temp_r!=NULL)
            {
                if(self_join_middle_bucket_parallel(join, table_r, table_s, &(list_temp_r->bucket), &(list_temp_s->bucket), result_R, result_S, index_list, &r_index, &s_index, &counter))
                {
                    fprintf(stderr, "execute_query: Error in self_join_middle_bucket\n");
                    return -10;
                }
                if(r_index>=list_temp_r->bucket.index_to_add_next)
                {
                    list_temp_r=list_temp_r->next;
                    r_index=0;
                }
                if(s_index>=list_temp_s->bucket.index_to_add_next)
                {
                    list_temp_s=list_temp_s->next;
                    s_index=0;
                }
            }
#endif
            //Update the lists and delete the old ones
            delete_middle_list(p->middle->tables[join->r.table_id].list);
            p->middle->tables[join->r.table_id].list=result_R;
            delete_r=0;
            delete_middle_list(p->middle->tables[join->s.table_id].list);
            p->middle->tables[join->s.table_id].list=result_S;
            delete_s=0;
            //TODO Check return code
            update_related_lists(p->pred_index, p->query, p->joined_tables, p->middle, delete_r, delete_s, result_R, result_S, index_list);
        }
        //Case C. Original Self-Join
        else if(p->query->predicates[p->pred_index].type==Self_Join&&
                (((predicate_join *) p->query->predicates[p->pred_index].p)->r.table_id==((predicate_join *) p->query->predicates[p->pred_index].p)->s.table_id))
        {
            predicate_join *join=p->query->predicates[p->pred_index].p;
            //find original tables
            table *original_table=get_table(p->tables, p->query->table_ids[join->r.table_id]);
            if(original_table==NULL)
            {
                fprintf(stderr, "execute_query: Table not found\n");
                return -12;
            }
#if defined(SERIAL_EXECUTION)||defined(SERIAL_SELFJOIN)
            if(p->middle->tables[join->r.table_id].list==NULL)
            {
                p->middle->tables[join->r.table_id].list=create_middle_list();
                if(self_join_table(join, original_table, p->middle->tables[join->r.table_id].list))
                {
                    fprintf(stderr, "execute_query: Error in self_join_table\n");
                    return -13;
                }
            }
            else
            {
                middle_list *new_list=create_middle_list();
                middle_list_node *list_temp=p->middle->tables[join->r.table_id].list->head;
                while(list_temp!=NULL)
                {
                    if(original_self_join_middle_bucket(join, &(list_temp->bucket), original_table, new_list))
                    {
                        fprintf(stderr, "execute_query: Error in original_self_join_middle_bucket\n");
                        return -14;
                    }
                    list_temp=list_temp->next;
                }
                delete_middle_list(p->middle->tables[join->r.table_id].list);
                p->middle->tables[join->r.table_id].list=new_list;
            }
#else
            if(p->middle->tables[join->r.table_id].list==NULL)
            {
                //TODO Figure out size of parts
                uint64_t parts;
                if(original_table->rows<SELFJOIN_TUPLES)
                {
                    parts=1;
                }
                else
                {
                    parts=original_table->rows/SELFJOIN_TUPLES+1;
                }
                uint64_t small_size=original_table->rows/parts;
                uint64_t extra=original_table->rows%parts;
                p->r_counter=parts;
                //Create a list_array to keep the results of each part
                list_array *la=create_list_array(parts, 1);
                if(la==NULL)
                {
                    fprintf(stderr, "execute_query: error in create_list_array\n");
                    return -1;
                }
                uint64_t start_index, end_index=0;
                for(uint64_t i=0; i<parts; i++)
                {
                    start_index=end_index;
                    end_index=start_index+small_size;
                    if(i<extra)
                    {
                        end_index++;
                    }
                    //Create and schedule the filter job
                    //TODO Check all job creations
                    job *newjob=create_filter_table_job(original_table, start_index, end_index, &p->r_counter, &p->r_mutex, la, i, p);
                    newjob->run=run_original_self_join_table_job;
                    schedule_fast_job(p->this_job->scheduler, newjob);
                }
                return 0;
            }
            else
            {
                //TODO Figure out size of parts
                uint64_t parts;
                if(p->middle->tables[join->r.table_id].list->number_of_nodes<5)
                {
                    parts=1;
                }
                else
                {
                    parts=p->middle->tables[join->r.table_id].list->number_of_nodes/SELFJOIN_FILTER_MIDDLE_BUCKETS+1;
                }
                uint64_t small_size=p->middle->tables[join->r.table_id].list->number_of_nodes/parts;
                uint64_t extra=p->middle->tables[join->r.table_id].list->number_of_nodes%parts;
                p->r_counter=parts;
                //Create a list_array to keep the results of each part
                list_array *la=create_list_array(parts, 1);
                if(la==NULL)
                {
                    fprintf(stderr, "execute_query: error in create_list_array\n");
                    return -1;
                }
                middle_list_node* job_node=p->middle->tables[join->r.table_id].list->head;
                middle_list_node* temp_node=NULL;
                uint64_t node_count=0;
                for(uint64_t i=0; i<parts&&job_node!=NULL; i++)
                {
                    node_count=small_size;
                    if(i<extra)
                    {
                        node_count++;
                    }
                    //Create and schedule the filter job
                    job *newjob=create_filter_middle_job(original_table, job_node, node_count, &p->r_counter, &p->r_mutex, la, i, p);
                    newjob->run=run_original_self_join_middle_job;
                    schedule_fast_job(p->this_job->scheduler, newjob);
                    temp_node=job_node;
                    while(node_count>0)
                    {
                        temp_node=temp_node->next;
                        node_count--;
                    }
                    job_node=temp_node;
                }
                return 0;
            }
#endif
        }
        else
        {
            fprintf(stderr, "execute_query: Undefined predicate type\n");
            return -15;
        }
        p->pred_index++;
    }
#if defined(SERIAL_EXECUTION)||defined(SERIAL_PROJECTIONS)
    for(uint32_t i=0; i<p->query->number_of_projections; i++)
    {
        projection *pr=&p->query->projections[i];
        if(p->middle->tables[pr->column_to_project.table_id].list==NULL||p->middle->tables[pr->column_to_project.table_id].list->number_of_nodes==0)
        {
#if defined(SORTED_PROJECTIONS)
            if(store_projection_in_scheduler(p->this_job->scheduler, p->query_id, p->query->number_of_projections, i, 0)!=0)
            {
                return -4;
            }
#else
            fprintf(stderr, "\e[1;33mNULL \e[0m");
#endif
            continue;
        }
        table *original_table=get_table(p->tables, p->query->table_ids[pr->column_to_project.table_id]);
        if(original_table==NULL)
        {
            fprintf(stderr, "calculate_projections: Table not found\n");
            return -5;
        }
        uint64_t sum=0;
        middle_list_node *list_temp=p->middle->tables[pr->column_to_project.table_id].list->head;
        while(list_temp!=NULL)
        {
            calculate_sum(pr, &(list_temp->bucket), original_table, &sum);
            list_temp=list_temp->next;
        }
#if defined(SORTED_PROJECTIONS)
        if(store_projection_in_scheduler(p->this_job->scheduler, p->query_id, p->query->number_of_projections, i, sum)!=0)
        {
            return -4;
        }
#else
        fprintf(stderr, "\e[1;33m%" PRIu64 " \e[0m", sum);
#endif
    }
#if !defined(SORTED_PROJECTIONS)
    fprintf(stderr, "\n");
#endif
#else
    p->r_counter=p->query->number_of_projections; //Used In The projections to count how many have left
    uint32_t number_of_projections=p->query->number_of_projections;
    for(uint32_t i=0; i<number_of_projections; i++)
    {
        job* newjob=create_projection_job(p, &p->r_mutex, &p->r_counter, &p->query->projections[i], i);
        if(newjob==NULL)
        {
            destroy_query_job((void*) p);
            return -2;
        }
        //Append to fifo
        //TODO Add checks
        schedule_fast_job(p->this_job->scheduler, newjob);
    }
#endif
#if defined(SERIAL_PROJECTIONS)
    destroy_query_job((void*)p);
#endif
    return 0;
}
void calculate_sum(projection* p, middle_list_bucket *bucket, table *table, uint64_t *sum)
{
    for(unsigned int i=0; i<bucket->index_to_add_next; i++)
    {
        *sum+=table->array[p->column_to_project.column_id][bucket->row_ids[i]];
    }
}
