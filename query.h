#ifndef QUERY_H
#define QUERY_H

#include "middle_list.h"
#include "table.h"
typedef enum predicate_type
{
    Join, Self_Join, Filter
} predicate_type;
typedef enum predicate_filter_type
{
    Not_Specified, Less, Less_Equal, Equal, Greater, Greater_Equal, Not_Equal
} predicate_filter_type;
/**
 * Contains a table index and a row index.
 */
typedef struct table_column
{
    uint32_t table_id;
    uint64_t column_id;
} table_column;
/**
 * The join predicate.
 * Contains the two table id/column id pairs that are used in the join.
 */
typedef struct predicate_join
{
    table_column r;
    table_column s;
} predicate_join;
/**
 * The filter predicate.
 * Contains a table id/column id pair, the type of filter and the value to use.
 */
typedef struct predicate_filter
{
    table_column r;
    predicate_filter_type filter_type;
    uint64_t value;
} predicate_filter;
/**
 * The struct that represents a predicate.
 * Contains the type of the predicate and a void* pointer to a predicate_filter
 * or a predicate_join.
 */
typedef struct predicate
{
    predicate_type type;
    void* p;
} predicate;
/**
 * Contains the table id and the row id to use in the projection.
 */
typedef struct projection
{
    table_column column_to_project;

} projection;
/**
 * The struct that represents a query.
 * It has an array of table ids (0,4,3 etc), an array of predicates and an
 * array of projections.
 */
typedef struct query
{
    uint32_t number_of_tables;
    uint32_t*table_ids;
    uint32_t number_of_predicates;
    predicate* predicates;
    uint32_t number_of_projections;
    projection* projections;
} query;
/**
 * Creates and initializes an empty query.
 * @return query* A pointer to the created query
 */
query* create_query(void);
/**
 * Deletes a query.
 * @param query* A pointer to the query
 */
void delete_query(query*);
/**
 * Accepts query as a char* analyzes it and stores it in the pointer.
 * @param char* The query as a string (Not NULL)
 * @param query* Where to store the data of the query (Not NULL, must be valid)
 * @return 0 On succes
 */
int analyze_query(char*, query*);
/**
 * Accepts a query and checks if it is valid with the tables.
 * @param query* The query to validate
 * @param table_index* The tables
 * @return 0 On succes
 */
int validate_query(query*, table_index*);
/**
 * Optimizes a query by rearranging the predicates
 * @param query* The query to optimize
 * @param tale_index* The tables
 * @return 0 On succes
 */
int optimize_query(query*q,table_index *);

/**
 * Creates an array of bool that show which tables in a predicate must be sorted
 * @param query* The query
 * @param bool** A pointer to create an array of bools that show which
 * table.column in the join predicates need to be sorted
 * @return 0 On succes
 */
int create_sort_array(query*q,bool**t_c_to_sort);
/**
 * Moves the joins/self joins just before they are needed for best
 * memory consumption
 * @param query* Pointer to the query to optimize
 * @return 0 On succes
 */
int optimize_query_memory(query*);
/**
 * Prints all data of the query
 * @param query* Pointer to the query to print
 */
void print_query(query*);
/**
 * Prints all data of the query as the str given
 * @param query* Pointer to the query to print
 */
void print_query_like_an_str(query* q);

typedef struct{
  middle_list *list;
}middle_table;

typedef struct{
  uint32_t number_of_tables;
  middle_table *tables;
}middleman;



/**
 * Executes a query
 * @param Pointer to the query
 */
middleman *execute_query(query *, table_index *, bool *);

void calculate_projections(query *, table_index* , middleman *);

#endif /* QUERY_H */
