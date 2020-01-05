#ifndef PROJECTION_LIST_H
#define PROJECTION_LIST_H
#include <inttypes.h>
typedef struct projection_node projection_node;

typedef struct projection_list
{
    projection_node* head; //The first node of the list
    projection_node* tail; //The last node of the list
    uint32_t number_of_nodes;
}projection_list;

projection_list* create_projection_list(void);
void delete_projection_list(projection_list*);
int append_to_projection_list(projection_list*, uint64_t, uint32_t, uint32_t, uint64_t);
void print_projection_list(projection_list*);
#endif /* PROJECTION_LIST_H */
