#ifndef QUERY_H
#define QUERY_H
#include "relation.h"

enum predicate_type{Join,Filter};
enum predicate_filter_type{Less,Less_Equal,Equal,More,More_Equal,Not_Equal};

/**
 * Contains a table id and a row id.
 */
typedef struct table_column
{
    unsigned int table_id;
    unsigned int column_id;
}table_column;
/**
 * The join predicate.
 * Contains the two table id/column id pairs that are used in the join.
 */
typedef struct predicate_join
{
    table_column r;
    table_column s;
}predicate_join;
/**
 * The filter predicate.
 * Contains a table id/column id pair, the type of filter and the value to use.
 */
typedef struct predicate_filter
{
    table_column r;
    predicate_filter_type filter_type;
    uint64_t value;
}predicate_filter;
/**
 * The struct that represents a predicate.
 * Contains the type of the predicate and a void* pointer to a predicate_filter
 * or a predicate_join.
 */
typedef struct predicate
{
    predicate_type type;
    void* p;
}predicate;
/**
 * Contains the table id and the row id to use in the projection.
 */
typedef struct projection
{
    table_column column_to_project;

}projection;

/**
 * The struct that represents a query.
 * It has an array of table ids (0,4,3 etc), an array of predicates and an
 * array of projections.
 */
typedef struct query
{
    unsigned int number_of_tables;
    unsigned int* table_ids;
    unsigned int number_of_predicates;
    predicate* predicates;
    unsigned int number_of_projections;
    projection* projections;
}query;

#endif /* QUERY_H */
