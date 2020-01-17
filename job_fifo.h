#ifndef JOB_FIFO_H
#define JOB_FIFO_H
#include <inttypes.h>
#include <stdbool.h>
#define JOB_FIFO_BUCKET_SIZE 256
typedef struct job job;
typedef struct job_fifo_bucket
{
    job* jobs[JOB_FIFO_BUCKET_SIZE];
    uint32_t index_to_add_next;
    uint32_t index_to_remove_next;
} job_fifo_bucket;
typedef struct job_fifo_node job_fifo_node;
typedef struct job_fifo_node
{
    job_fifo_bucket bucket;
    job_fifo_node* next;
} job_fifo_node;
typedef struct job_fifo
{
    job_fifo_node* head;    //The first node of the fifo list
    job_fifo_node* tail;    //The last node of the fifo list
    job_fifo_node* append_node; //The node of the fifo to add jobs
    uint32_t number_of_nodes;   //Counter of the nodes
    uint64_t number_of_jobs;    //Counter of jobs
} job_fifo;
/**
 * Creates and initializes a new empty job fifo node (with the bucket)
 * @return job_fifo_node* The new node
 */
job_fifo_node* create_job_fifo_node();
/**
 * Checks if the bucket given is full
 * @param job_fifo_bucket The bucket to check
 * @return bool true if the bucket is full else false
 */
bool is_job_fifo_bucket_full(job_fifo_bucket*);
/**
 * Appends the rowids given to the bucket.
 * @param job_fifo_bucket* the bucket
 * @param job* the job to append
 * @return 0 if successful
 */
int append_to_job_fifo_bucket(job_fifo_bucket*, job*);
/**
 * Prints the contents of the bucket (index and array)
 * @param job_fifo_bucket the bucket to print
 */
void print_job_fifo_bucket(job_fifo_bucket*);
/**
 * Creates an empty job fifo and returns a pointer to that fifo
 * @return job_fifo* The new fifo
 */
job_fifo* create_job_fifo();
/**
 * Deletes all the nodes of the job_fifo given. If a job is not NULL then a
 * message is printed
 * @param job_fifo* the list to delete
 */
void delete_job_fifo(job_fifo*);
/**
 * Adds a job in the fifo.
 * @param job_fifo* The fifo to add the job
 * @param job* the job to add
 * @return int 0 If Successful
 */
int append_to_job_fifo(job_fifo*, job*);
/**
 * Prints all the nodes and buckets of the fifo from first to last.
 * @param job_fifo* The job_fifo to print
 */
void print_job_fifo(job_fifo*);
/**
 * Returns if the job_fifo is empty.
 * @param job_fifo* the fifo
 * @return bool true if empty else false
 */
bool is_job_fifo_empty(job_fifo*);
/**
 * Returns the next job from the fifo.
 * @param job_fifo* The fifo to remove the job
 * @return job* NULL if error occured
 */
job* pop_from_job_fifo(job_fifo*);
#endif /* JOB_FIFO_H */
