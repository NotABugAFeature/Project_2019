#ifndef EXECUTE_QUERY_H
#define EXECUTE_QUERY_H
#include "relation.h"
#include "query.h"
#include "job_scheduler.h"
middleman *initialize_middleman(uint32_t number_of_tables);
int filter_middle_bucket(predicate_filter *filter,middle_list_bucket *bucket,table* table,middle_list *new_list);
int filter_original_table(predicate_filter *filter,table* table,middle_list *new_list);
relation *construct_relation_from_table(table * table, uint64_t column_id);
void construct_relation_from_middleman(middle_list_bucket *bucket,table *table,relation *rel,uint64_t column_id,uint64_t *counter);
int original_self_join_middle_bucket(predicate_join *,middle_list_bucket*,table*,middle_list*);
int update_related_lists(uint32_t , query *, uint32_t **, middleman *, int , int ,middle_list *, middle_list *, middle_list *);
int self_join_middle_bucket(predicate_join *join,table *table_r,table *table_s,middle_list_bucket *bucket_r,
                            middle_list_bucket *bucket_s,middle_list *list_r,middle_list *list_s,middle_list *index_list,
                            uint32_t *counter);
#if defined(SERIAL_EXECUTION)||(defined(SERIAL_JOIN)&&defined(SERIAL_FILTER)&&defined(SERIAL_SELFJOIN))
int update_middle_bucket(middle_list_bucket **lookup, middle_list_bucket *bucket, middle_list *updated_list);
#else
int update_middle_bucket(lookup_table*lookup, middle_list_bucket *bucket, middle_list *updated_list);
#endif
int self_join_table(predicate_join *join, table* table, middle_list *list);
int filter_original_table_parallel(predicate_filter *filter,table* table,uint64_t start_index,uint64_t end_index,middle_list *new_list);
int self_join_table_parallel(predicate_join *join, table* table,uint64_t start,uint64_t end, middle_list *list);
int execute_query_parallel(job_query_parameters* p);
int self_join_middle_bucket_parallel(predicate_join *, table *, table *, middle_list_bucket *,middle_list_bucket *, middle_list *, middle_list *,
        middle_list *, uint32_t *, uint32_t *, uint32_t *);
void calculate_sum(projection *p, middle_list_bucket *bucket, table *table, uint64_t *sum);
#endif
