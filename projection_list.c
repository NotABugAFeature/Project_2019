#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include "projection_list.h"
typedef struct projection_node
{
    uint64_t query_id;
    uint32_t number_of_projections;
    uint64_t* projections;
    projection_node* next;

} projection_node;
projection_node* create_projection_node(uint64_t query_id, uint32_t number_of_projections
                                        , uint32_t projection_index, uint64_t projection_sum)
{
    if(number_of_projections==0||projection_index>=number_of_projections)
    {
        fprintf(stderr, "create_projection_node: Error with the parameters\n");
        return NULL;
    }
    //Create the node
    projection_node* new_node;
    new_node=malloc(sizeof(projection_node));
    if(new_node==NULL)
    {
        perror("create_projection_node: error in malloc");
        return NULL;
    }
    //Initialize with the values
    new_node->projections=malloc(sizeof(uint64_t)*number_of_projections);
    if(new_node->projections==NULL)
    {
        perror("create_projection_node: error in projection array malloc");
        free(new_node);
        return NULL;
    }
    new_node->query_id=query_id;
    new_node->number_of_projections=number_of_projections;
    new_node->projections[projection_index]=projection_sum;
    new_node->next=NULL;
    return new_node;
}
void delete_projection_node(projection_node* node)
{
    if(node==NULL)
    {
        fprintf(stderr, "delete_projection_node: NULL parameter\n");
        return;
    }
    if(node->projections!=NULL)
    {
        free(node->projections);
        node->projections=NULL;
    }
    free(node);
    node=NULL;
}
void print_projection_node(projection_node* node)
{
    if(node==NULL||node->projections==NULL)
    {
        fprintf(stderr, "delete_projection_node: NULL parameter\n");
        return;
    }
    for(uint32_t i=0; i<node->number_of_projections; i++)
    {
        if(node->projections[i]==0)
        {
            fprintf(stderr, "\e[1;33mNULL \e[0m");
        }
        else
        {
            fprintf(stderr, "\e[1;33m%" PRIu64 " \e[0m", node->projections[i]);
        }
    }
    fprintf(stderr, "\n");
}
projection_list* create_projection_list(void)
{
    //Create the list
    projection_list* new_list;
    new_list=malloc(sizeof(projection_list));
    if(new_list==NULL)
    {
        perror("create_projection_list: error in malloc");
        return NULL;
    }
    //Initialize empty
    new_list->head=NULL;
    new_list->tail=NULL;
    new_list->number_of_nodes=0;
    return new_list;
}
void delete_projection_list(projection_list* list)
{
    if(list==NULL)
    {
        fprintf(stderr, "delete_projection_list: NULL list pointer\n");
        return;
    }
    projection_node* temp=list->head;
    //Delete all the nodes
    while(list->head!=NULL)
    {
        list->head=temp->next;
        //Delete the node
        delete_projection_node(temp);
        temp=list->head;
        list->number_of_nodes--;
    }
    free(list);
}
void print_projection_list(projection_list* list)
{
    if(list==NULL)
    {
        fprintf(stderr, "print_projection_list: NULL list pointer\n");
        return;
    }
    printf("Total queries: %"PRIu32"\n",list->number_of_nodes);
    projection_node* temp=list->head;
    while(temp!=NULL)//Visit All The Nodes And Print Them
    {
        print_projection_node(temp);
        temp=temp->next;
    }
}
int append_to_projection_list(projection_list* list, uint64_t query_id,
    uint32_t number_of_projections,uint32_t projection_index, uint64_t projection_sum)
{
    if(list->head==NULL)//Append as first node
    {
        projection_node* newnode=create_projection_node(query_id,number_of_projections,projection_index,projection_sum);
        if(newnode==NULL)
        {
            return -1;
        }
        list->head=newnode;
        list->tail=newnode;
        list->number_of_nodes++;
        return 0;
    }
    else//Search the list to find the position
    {
        if(list->tail==NULL||list->tail->next!=NULL)
        {
            fprintf(stderr, "append_to_projection_list: error of the list\n");
            return -2;
        }
        else
        {
            if(list->tail->query_id<query_id)
            {//Append as last node
                projection_node* newnode=create_projection_node(query_id,number_of_projections,projection_index,projection_sum);
                if(newnode==NULL)
                {
                    return -1;
                }
                list->tail=newnode;
                list->number_of_nodes++;
                return 0;
            }
            else if(list->tail->query_id==query_id)
            {//Update tail
                if(list->tail->number_of_projections>projection_index)
                {
                    list->tail->projections[projection_index]=projection_sum;
                    return 0;
                }
                else
                {
                    fprintf(stderr, "append_to_projection_list: list->tail->number_of_projections <= projection_index\n");
                    return -3;
                }
            }
            projection_node* temp;
            temp=list->head;
            if(temp->query_id>query_id)//Check if newnode<head
            {
                projection_node* newnode=create_projection_node(query_id,number_of_projections,projection_index,projection_sum);
                if(newnode==NULL)
                {
                    return -1;
                }
                newnode->next=list->head;
                list->head=newnode;
                list->number_of_nodes++;
                return 0;
            }
            else if(temp->query_id==query_id)
            {//Update head
                if(temp->number_of_projections>projection_index)
                {
                    temp->projections[projection_index]=projection_sum;
                    return 0;
                }
                else
                {
                    fprintf(stderr, "append_to_projection_list: head->number_of_projections <= projection_index\n");
                    return -3;
                }
            }
            while(temp->next!=NULL)//Search the list to find the position
            {
                if(temp->next->query_id>query_id)
                {//Append node
                    projection_node* newnode=create_projection_node(query_id,number_of_projections,projection_index,projection_sum);
                    if(newnode==NULL)
                    {
                        return -1;
                    }
                    newnode->next=temp->next;
                    temp->next=newnode;
                    list->number_of_nodes++;
                    return 0;
                }
                else if(temp->next->query_id==query_id)
                {//Update temp
                    if(temp->next->number_of_projections>projection_index)
                    {
                        temp->next->projections[projection_index]=projection_sum;
                        return 0;
                    }
                    else
                    {
                        fprintf(stderr, "append_to_projection_list: temp->next->number_of_projections <= projection_index\n");
                        return -3;
                    }
                }
                temp=temp->next;
            }
        }
    }
    return -4;
}
