#include <stdio.h>
#include <stdlib.h>
#include "job_fifo.h"
#include "job_scheduler.h"
job_fifo_node* create_job_fifo_node()
{
    //Create the node
    job_fifo_node* new_node;
    new_node=malloc(sizeof(job_fifo_node));
    if(new_node==NULL)
    {
        perror("job_fifo_node: error in malloc");
        return NULL;
    }
    //Initialize empty
    new_node->next=NULL;
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE; i++)
    {
        new_node->bucket.jobs[i]=NULL;
    }
    new_node->bucket.index_to_add_next=0;
    new_node->bucket.index_to_remove_next=0;
    return new_node;
}
bool is_job_fifo_bucket_full(job_fifo_bucket* b)
{
    return b->index_to_add_next==JOB_FIFO_BUCKET_SIZE ? true : false;
}
int append_to_job_fifo_bucket(job_fifo_bucket* b, job* j)
{
    if(j==NULL)
    {
        return -1;
    }
    if(!is_job_fifo_bucket_full(b))
    {
        b->jobs[b->index_to_add_next]=j;
        b->index_to_add_next++;
        return 0;
    }
    return 1;
}
void print_job_fifo_bucket(job_fifo_bucket* b)
{
    if(b==NULL)
    {
        fprintf(stderr, "print_job_fifo_bucket: NULL Parameter\n");
        return;
    }
    //Print the array inside the bucket
    printf("index_to_add_next: %" PRIu32 "\n", b->index_to_add_next);
    printf("index_to_remove_next: %" PRIu32 "\n", b->index_to_remove_next);
    for(uint32_t i=0; i<b->index_to_add_next; i++)
    {
        print_job(b->jobs[i]);
    }
}
job_fifo* create_job_fifo()
{
    //Create the fifo
    job_fifo* new_fifo;
    new_fifo=malloc(sizeof(job_fifo));
    if(new_fifo==NULL)
    {
        perror("create_job_fifo(): error in malloc");
        return NULL;
    }
    //Initialize the fifo to be empty
    new_fifo->head=NULL;
    new_fifo->append_node=NULL;
    new_fifo->tail=NULL;
    new_fifo->number_of_nodes=0;
    new_fifo->number_of_jobs=0;
    return new_fifo;
}
void delete_job_fifo(job_fifo* fifo)
{
    if(fifo==NULL)
    {
        fprintf(stderr, "delete_job_fifo: NULL fifo pointer\n");
        return;
    }
    job_fifo_node* temp=fifo->head;
    //Delete all the nodes
    while(fifo->head!=NULL)
    {
        fifo->head=temp->next;
        //Check for jobs in the node
        if(temp->bucket.index_to_add_next!=temp->bucket.index_to_remove_next)
        {
            fprintf(stderr, "delete_job_fifo: jobs not deleted\n");
            for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE; i++)
            {
                if(temp->bucket.jobs[i]!=NULL)
                {
                    fprintf(stderr, "delete_job_fifo: job not deleted\n");
                    delete_job(temp->bucket.jobs[i]);
                    temp->bucket.jobs[i]=NULL;
                }
            }
        }
        //Delete the node
        free(temp);
        temp=fifo->head;
        fifo->number_of_nodes--;
    }
    free(fifo);
}
void print_job_fifo(job_fifo* fifo)
{
    if(fifo==NULL)
    {
        fprintf(stderr, "print_job_fifo: NULL fifo pointer\n");
        return;
    }
    uint32_t index=0;
    job_fifo_node*temp=fifo->head;
    printf("Number Of Buckets: %"PRIu32"\n", fifo->number_of_nodes);
    printf("Number Of Records: %"PRIu64"\n", fifo->number_of_jobs);
    while(temp!=NULL)//Visit all the nodes and print them
    {
        printf("Bucket Index: %"PRIu32, index);
        if(fifo->append_node==temp)
        {
            printf("\tAppend Node");
        }
        printf("\n");
        print_job_fifo_bucket(&(temp->bucket));
        index++;
        temp=temp->next;
    }
}
int append_to_job_fifo(job_fifo* fifo, job* j)
{
    if(fifo==NULL||j==NULL)
    {
        fprintf(stderr, "append_to_job_fifo: NULL parameter\n");
        return 1;
    }
    if(fifo->head==NULL)//Create the first node
    {
        fifo->head=create_job_fifo_node();
        if(fifo->head==NULL)
        {
            return 1;
        }
        //No need to check
        if(append_to_job_fifo_bucket(&fifo->head->bucket, j)!=0)
        {//Will never happen
            fprintf(stderr, "append_to_job_fifo: Error cannot add to empty bucket\n");
            return 2;
        }
        fifo->append_node=fifo->head;
        fifo->tail=fifo->head;
        fifo->number_of_nodes++;
        fifo->number_of_jobs++;
    }
    else//Add to the append node
    {
        if(fifo->append_node==NULL||fifo->tail==NULL||fifo->tail->next!=NULL)
        {
            fprintf(stderr, "append_to_job_fifo: error of the fifo\n");
            return 3;
        }
        else
        {
            if(append_to_job_fifo_bucket(&fifo->append_node->bucket, j)!=0)
            {//Full Bucket
                if(fifo->append_node!=fifo->tail)
                {
                    if(fifo->append_node->next!=fifo->tail)
                    {
                        fprintf(stderr, "append_to_job_fifo: fifo->append_node->next!=fifo->tail\n");
                        return 4;
                    }
                    fifo->append_node=fifo->tail;
                    if(append_to_job_fifo_bucket(&fifo->append_node->bucket, j)!=0)
                    {
                        fprintf(stderr, "append_to_job_fifo: error of the fifo tail\n");
                        return 4;
                    }
                }
                else
                {
                    fifo->tail->next=create_job_fifo_node();
                    if(fifo->tail->next==NULL)
                    {
                        return 5;
                    }
                    fifo->tail=fifo->tail->next;
                    fifo->append_node=fifo->tail;
                    fifo->number_of_nodes++;
                    if(append_to_job_fifo_bucket(&fifo->tail->bucket, j))
                    {
                        fprintf(stderr, "append_to_job_fifo: error cannot add to empty bucket\n");
                        return 6;
                    }
                }
            }
            fifo->number_of_jobs++;
        }
    }
    return 0;
}
bool is_job_fifo_empty(job_fifo* fifo)
{
    return fifo->number_of_jobs==0 ? true : false;
}
job* pop_from_job_fifo(job_fifo* fifo)
{
    if(fifo==NULL)
    {
        fprintf(stderr, "pop_from_job_fifo: NULL parameter\n");
        return NULL;
    }
    if(is_job_fifo_empty(fifo))
    {
        fprintf(stderr, "pop_from_job_fifo: empty fifo\n");
        return NULL;
    }
    //Pop the job from head
    job* temp_job=fifo->head->bucket.jobs[fifo->head->bucket.index_to_remove_next];
    fifo->head->bucket.jobs[fifo->head->bucket.index_to_remove_next]=NULL;
    fifo->head->bucket.index_to_remove_next++;
    fifo->number_of_jobs--;
    if(fifo->append_node==fifo->head)
    {//Restart the indexes if remove_index==append_index
        if(fifo->head->bucket.index_to_remove_next==fifo->head->bucket.index_to_add_next)
        {
            if(fifo->number_of_jobs!=0)
            {
                fprintf(stderr, "pop_from_job_fifo: error of fifo\n");
                return temp_job;
            }
            fifo->head->bucket.index_to_remove_next=0;
            fifo->head->bucket.index_to_add_next=0;
        }
    }
    else//More than one nodes
    {
        if(fifo->head->bucket.index_to_remove_next==JOB_FIFO_BUCKET_SIZE)//Empty head
        {
            //TODO Remove this part after debugging
            for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE; i++)
            {
                if(fifo->head->bucket.jobs[i]!=NULL)
                {
                    fprintf(stderr, "pop_from_job_fifo: not null job in head\n");
                }
            }
            if(fifo->tail->bucket.index_to_add_next!=0||fifo->number_of_nodes==2)
            {//Add head as new tail
                //TODO Remove After Debugging
                if(fifo->append_node!=fifo->tail)
                {
                    fprintf(stderr, "pop_from_job_fifo: fifo->append_node!=fifo->tail\n");
                }
                job_fifo_node* temp=fifo->head;
                fifo->head=fifo->head->next;
                fifo->tail->next=temp;
                fifo->tail=fifo->tail->next;
                fifo->tail->next=NULL;
//                fifo->number_of_nodes--;
                fifo->tail->bucket.index_to_add_next=0;
                fifo->tail->bucket.index_to_remove_next=0;
            }
            else//Delete the head
            {
                job_fifo_node* temp=fifo->head;
                fifo->head=fifo->head->next;
                free(temp);
                fifo->number_of_nodes--;
            }
        }
    }
    return temp_job;
}
