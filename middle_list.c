#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include "middle_list.h"
/**
 * Creates and initializes a new empty middle list node (with the bucket)
 * @return middle_list_node* The new node
 */
struct middle_list_node* create_middle_list_node()
{
    //Create the node
    middle_list_node* new_node;
    new_node=malloc(sizeof(middle_list_node));
    if(new_node==NULL)
    {
        perror("middle_list_node: error in malloc");
        return NULL;
    }
    //Initialize empty
    new_node->next=NULL;
    new_node->bucket.index_to_add_next=0;
    return new_node;
}
/**
 * Checks if the bucket given is full
 * @param middle_list_bucket The bucket to check
 * @return int 1 if the bucket is full else 0
 */
int is_middle_list_bucket_full(middle_list_bucket* bucket)
{
    return bucket->index_to_add_next==middle_LIST_BUCKET_SIZE ? 1 : 0;
}
/**
 * Appends the given rowid to the bucket
 * @param middle_list_bucket* the bucket
 * @param uint64_t r_row_id
 * @return 0 if successful 1 else
 */
int append_to_middle_bucket(middle_list_bucket* bucket, uint64_t r_row_id)
{
    if(!is_middle_list_bucket_full(bucket))
    {
        bucket->row_ids[bucket->index_to_add_next]=r_row_id;
        bucket->index_to_add_next++;
        return 0;
    }
    return 1;
}
/**
 * Prints the contents of the bucket (index and array)
 * @param middle_list_bucket the bucket to print
 * @param FILE* Where the output will be printed
 */
void print_middle_bucket(middle_list_bucket* bucket, FILE*output)
{
    //Print the array inside the bucket
    for(unsigned int i=0; i<bucket->index_to_add_next; i++)
    {
        fprintf(output, "RowIdR: %" PRIu64 "\n", bucket->row_ids[i]);
    }
}
middle_list* create_middle_list()
{
    //Create the list
    middle_list* new_list;
    new_list=malloc(sizeof(middle_list));
    if(new_list==NULL)
    {
        perror("create_middle_list(): error in malloc");
        return NULL;
    }
    new_list->head=NULL;
    new_list->tail=NULL;
    new_list->number_of_nodes=0;
    new_list->number_of_records=0;
    //Initialize the list to have one node
    //    new_list->head=create_middle_list_node();
    //    if(new_list->head==NULL)
    //    {
    //        free(new_list);
    //        return NULL;
    //    }
    //    new_list->tail=new_list->head;
    //    new_list->number_of_nodes=1;
    //    new_list->number_of_records=0;
    return new_list;
}
void delete_middle_list(middle_list* list)
{
    if(list==NULL)
    {
        printf("delete_middle_list: NULL list pointer\n");
        return;
    }
    middle_list_node* temp=list->head;
    //Delete all the nodes
    while(list->head!=NULL)
    {
        list->head=temp->next;
        free(temp);
        temp=list->head;
        list->number_of_nodes--;
    }
    list->number_of_records=0;
    free(list);
}
void print_middle_list(middle_list* list, FILE*output)
{
    if(list==NULL)
    {
        fprintf(stderr, "print_middle_list: NULL list pointer\n");
        return;
    }
    unsigned int index=0;
    middle_list_node*temp=list->head;
    fprintf(output, "Number Of Records: %"PRIu64"\n", middle_list_get_number_of_records(list));
    fprintf(output, "Number Of Buckets: %u\n", list->number_of_nodes);
    while(temp!=NULL)//Visit all the nodes and print them
    {
        fprintf(output, "Bucket Index: %u\n", index);
        print_middle_bucket(&(temp->bucket), output);
        index++;
        temp=temp->next;
    }
}
#if defined(SERIAL_EXECUTION)||(defined(SERIAL_JOIN)&&defined(SERIAL_FILTER)&&defined(SERIAL_SELFJOIN))
middle_list_bucket **construct_lookup_table(middle_list* list)
{
    if(list==NULL)
    {
        fprintf(stderr, "print_middle_list: NULL list pointer\n");
        return NULL;
    }
    middle_list_bucket **lookup=malloc(middle_list_get_number_of_buckets(list)*sizeof(middle_list_bucket *));
    if(lookup==NULL)
    {
        fprintf(stderr, "print_middle_list: NULL list pointer\n");
        return NULL;
    }
    unsigned int i=0;
    middle_list_node*temp=list->head;
    while(temp!=NULL)//Visit all the nodes and print them
    {
        lookup[i]= &(temp->bucket);
        temp=temp->next;
        i++;
    }
    return lookup;
}
#else
lookup_table *construct_lookup_table(middle_list* list)
{
    if(list==NULL)
    {
        fprintf(stderr, "print_middle_list: NULL list pointer\n");
        return NULL;
    }
    lookup_table *lookup_t=malloc(sizeof(lookup_table));
    if(lookup_t==NULL)
    {
        perror("construct_lookup_table: malloc error\n");
        return NULL;
    }
    lookup_t->lookup_table=malloc(middle_list_get_number_of_buckets(list)*sizeof(middle_list_bucket *));
    if(lookup_t->lookup_table==NULL)
    {
        perror("construct_lookup_table: malloc error\n");
        free(lookup_t);
        return NULL;
    }
    lookup_t->min=malloc(middle_list_get_number_of_buckets(list)*sizeof(uint64_t));
    if(lookup_t->min==NULL)
    {
        perror("construct_lookup_table: malloc error\n");
        free(lookup_t->lookup_table);
        free(lookup_t);
        return NULL;
    }
    lookup_t->max=malloc(middle_list_get_number_of_buckets(list)*sizeof(uint64_t));
    if(lookup_t->max==NULL)
    {
        perror("construct_lookup_table: malloc error\n");
        free(lookup_t->lookup_table);
        free(lookup_t->min);
        free(lookup_t);
        return NULL;
    }
    lookup_t->size=list->number_of_nodes;
    uint64_t min=0;
    uint64_t max=0;
    unsigned int i=0;
    middle_list_node*temp=list->head;
    while(temp!=NULL)//Visit all the nodes find the min/max rowids
    {
        min=max;
        lookup_t->lookup_table[i]= &(temp->bucket);
        lookup_t->min[i]=min;
        max+=(temp->bucket.index_to_add_next-1);
        lookup_t->max[i]=max++;
        temp=temp->next;
        i++;
    }
    return lookup_t;
}
void delete_lookup_table(lookup_table* lt)
{
    if(lt==NULL)
    {
        fprintf(stderr, "delete_lookup_table: NULL pointer\n");
        return;
    }
    if(lt->lookup_table!=NULL)
    {
        free(lt->lookup_table);
        lt->lookup_table=NULL;
    }
    if(lt->max!=NULL)
    {
        free(lt->max);
        lt->max=NULL;
    }
    if(lt->min!=NULL)
    {
        free(lt->min);
        lt->min=NULL;
    }
    free(lt);
    lt=NULL;
}
#endif
int append_to_middle_list(middle_list* list, uint64_t r_row_id)
{
    if(list->head==NULL)//Create the first node
    {
        list->head=create_middle_list_node();
        if(list->head==NULL)
        {
            return 1;
        }
        //No need to check
        if(append_to_middle_bucket(&list->head->bucket, r_row_id))
        {
            printf("append_to_middle_list: Error cannot add to empty bucket\n");
            return 2;
        }
        list->tail=list->head;
        list->number_of_nodes++;
    }
    else//Add to the tail
    {
        if(list->tail==NULL||list->tail->next!=NULL)
        {
            printf("append_to_middle_list: error of the list\n");
            return 3;
        }
        else
        {
            if(append_to_middle_bucket(&list->tail->bucket, r_row_id))
            {//Full Bucket
                list->tail->next=create_middle_list_node();
                //No Need To Check
                list->tail=list->tail->next;
                if(append_to_middle_bucket(&list->tail->bucket, r_row_id))
                {
                    printf("append_to_middle_list: Error cannot add to empty bucket\n");
                    return 4;
                }
                list->number_of_nodes++;
            }
        }
    }
    list->number_of_records++;
    return 0;
}
int is_middle_list_empty(middle_list* list)
{
    //return list->Head==NULL ? 1 : 0;
    return list->number_of_nodes==0 ? 1 : 0;
}
unsigned int middle_list_get_number_of_buckets(middle_list* list)
{
    return list->number_of_nodes;
}
uint64_t middle_list_get_number_of_records(middle_list* list)
{
    if(list==NULL||list->number_of_nodes==0||list->tail==NULL)
    {
        return 0;
    }
    return list->number_of_records;
    //    return((list->number_of_nodes-1)*middle_LIST_BUCKET_SIZE+list->tail->bucket.index_to_add_next);
}
