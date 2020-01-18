#ifndef JOB_SCHEDULER_H
#define JOB_SCHEDULER_H
#include <stdio.h>
#include <inttypes.h>
#include <semaphore.h>
#include "job_fifo.h"
#include "table.h"
#include "query.h"
#include "queue.h"
#include "relation.h"
#include "list_array.h"
#include "projection_list.h"
#define JOIN_TUPLES 262144
#define FILTER_TUPLES 262144
#define SELFJOIN_TUPLES 262144
#define FILTER_MIDDLE_BUCKETS 16
#define SELFJOIN_FILTER_MIDDLE_BUCKETS 16
typedef struct job_scheduler
{
    job_fifo* fast_fifo;                //fifo that holds the fast jobs
    uint64_t fast_job_count;            //Counter of the jobs inside the fast fifo
    uint32_t thread_count;              //Counter of the threads using the fifo
    pthread_mutex_t job_fifo_mutex;     //Mutex for accessing the job fifo
    sem_t fifo_job_counter_sem;         //Semaphore that counts the items in the job fifo
#if defined(SORTED_PROJECTIONS)
    projection_list* projection_list;   //A sorted list that holds the projection results
#endif
    sem_t fifo_query_executing_sem;     //Semaphore that counts the queries in the job fifo
    sem_t threads_finished_sem;         //Semaphore that counts the finished threads
    bool terminate;                     //The execution is finished
} job_scheduler;
typedef struct job
{
    job_scheduler* scheduler;   //Pointer to the job_scheduler
    int (*run)(void*);          //The run function
    void (*destroy)(void*);     //The destroy function (free resources)
    void* parameters;           //The parameters of the job
} job;
typedef struct job_query_parameters //For each query one is created
{
    uint64_t query_id;          //query id (used for sorted projections)
    char* query_str;            //The query str to analyze
    query* query;               //The query
    table_index *tables;        //The tables (used in the execution of the query)
    middleman* middle;          //Middleman that stores the results
    bool* bool_array;           //An array that keeps track if we need to sort the relations
    uint32_t b_index;           //Index for the bool array
    uint32_t** joined_tables;   //The joined tables
    uint32_t pred_index;        //Which predicate is executed
    relation* r;                //R relation (needed in join)
    relation* s;                //S relation (needed in join)
    pthread_mutex_t q_mutex;    //A mutex used in scheduling the prejoin job
    bool is_prejoin_scheduled;  //used in scheduling the prejoin job
    pthread_mutex_t r_mutex;    //A mutex used in sorting/join/projections
    pthread_mutex_t s_mutex;    //A mutex used in sorting
    uint64_t r_counter;         //A counter used in sorting/join/projections
    uint64_t s_counter;         //A counter used in sorting
    job* this_job;
} job_query_parameters;
typedef struct job_presort_parameters
{
    relation** r;               //The relation (that will be sorted)
    relation* r_s;              //The auxiliary
    bool sort;                  //If we need to sort the relation
    pthread_mutex_t *mutex;     //mutex to inform the sorting is done
    uint64_t* unsorted_rows;    //Counter of the rows not sorted
    table_column* join;         //The table id and column id to sort
    job_query_parameters *q_params;
    job* this_job;
} job_presort_parameters;
typedef struct job_sort_parameters
{
    relation* r;                //The relation (that will be sorted)
    relation* r_s;              //The auxiliary
    pthread_mutex_t *mutex;     //mutex to inform the sorting is done
    uint64_t* unsorted_rows;  //Counter of the rows not sorted
    job* this_job;
    job_query_parameters *q_params;
    window win;                 //The window to sort
} job_sort_parameters;
typedef struct job_join_parameters
{
    uint64_t start_r;           //Index in the r relation for where to start the join
    uint64_t end_r;             //Index in the r relation for where to stop the join
    uint64_t start_s;           //Index in the s relation for where to start the join
    uint64_t *unjoined_parts;   //Counter of the join jobs not finished
    pthread_mutex_t *parts_mutex;   //mutex used to inform the join is finished
    uint64_t list_position;     //In which index to store the result
    list_array *lists;          //Result lists
    job_query_parameters *exe_params;
    job* this_job;
} job_join_parameters;
typedef struct job_filter_table_parameters
{
    table* table;               //Table to take the values from
    uint64_t start_index;       //Index in the table for where to start the filter
    uint64_t end_index;         //Index in the table for where to stop the filter
    uint64_t *unfiltered_parts; //Counter of the filter jobs not finished
    pthread_mutex_t *parts_mutex;   //mutex used to inform the filter is finished
    list_array *lists;          //Result lists
    uint32_t list_position;     //In which index to store the result
    job_query_parameters *exe_params;
    job* this_job;
} job_filter_table_parameters;
typedef struct job_filter_middle_parameters
{
    table* table;               //Table to take the values from
    middle_list_node* start_node;//The starting node of the middle results to filter
    uint32_t node_count;        //How many nodes to filter
    uint64_t *unfiltered_parts; //Counter of the filter jobs not finished
    pthread_mutex_t *parts_mutex;   //mutex used to inform the filter is finished
    list_array *lists;          //Result lists
    uint32_t list_position;     //In which index to store the result
    job_query_parameters *exe_params;
    job* this_job;
} job_filter_middle_parameters;
typedef struct job_projection_parameters
{
    projection* projection;     //The pojection to do
    uint32_t projection_index;  //The projection index
    uint64_t* projections_left; //Counter of the projection jobs not finished
    pthread_mutex_t *mutex;     //mutex used to inform the projection is finished
    job_query_parameters* query_parameters;
    job* this_job;
} job_projection_parameters;
/**
 * The function a thread will run to analyze/optimize a query
 * @param void* the parameters parameters
 * @return int 0 if no errors occurred
 */
int run_query_job(void*);
/**
 * The function a thread will run to execute a query
 * @param void* the parameters parameters
 * @return int 0 if no errors occurred
 */
int run_execute_job(void*);
/**
 * The function a thread will run to begin the sorting of a relation needed
 * for a join
 * @param void* the parameters parameters
 * @return int 0 if no errors occurred
 */
int run_presort_job(void*);
/**
 * The function a thread will run to sort a part of a relation
 * @param void* the parameters parameters
 * @return int 0 if no errors occurred
 */
int run_sort_job(void*);
/**
 * The function a thread will run to begin the join of two relations
 * @param void* the parameters parameters
 * @return int 0 if no errors occurred
 */
int run_prejoin_job(void*);
/**
 * The function a thread will run to join a part of the r relation with the s
 * relation
 * @param void* the parameters parameters
 * @return int 0 if no errors occurred
 */
int run_join_job(void*);
/**
 * The function a thread will run to filter a part of the table
 * @param void* the parameters parameters
 * @return int 0 if no errors occurred
 */
int run_filter_table_job(void*);
/**
 * The function a thread will run to filter a part of the middle results
 * @param void* the parameters parameters
 * @return int 0 if no errors occurred
 */
int run_filter_middle_job(void*);
/**
 * The function a thread will run to filter (self join) a part of the table
 * @param void* the parameters parameters
 * @return int 0 if no errors occurred
 */
int run_original_self_join_table_job(void*);
/**
 * The function a thread will run to filter (self join) a part of the middle results
 * @param void* the parameters parameters
 * @return int 0 if no errors occurred
 */
int run_original_self_join_middle_job(void*);
/**
 * The function a thread will run to find the projection result
 * @param void* the parameters parameters
 * @return int 0 if no errors occurred
 */
int run_projection_job(void*);
/**
 * Frees the resources used by the query job and inform the main thread that
 * a query is finished
 * @param void* parameters
 */
void destroy_query_job(void*);
/**
 * Frees the resources used by the join job
 * @param void* parameters
 */
void destroy_join_job(void*);
/**
 * Frees the resources used by the filter table job
 * @param void* parameters
 */
void destroy_filter_table_job(void*);
/**
 * Frees the resources used by the filter table job
 * @param void* parameters
 */
void destroy_filter_middle_job(void*);
/**
 * Frees the resources used by the presort job
 * @param void* parameters
 */
void destroy_presort_job(void* parameters);
/**
 * Frees the resources used by the sort job
 * @param void* parameters
 */
void destroy_sort_job(void* parameters);
/**
 * Frees the resources used by the projection job
 * @param void* parameters
 */
void destroy_projection_job(void* parameters);
/**
 * Creates and returns a query job
 * @param job_scheduler* the job scheduler the job will be added later
 * @param char* the query str to analyze
 * @param table_index* the tables of the database
 * @param uint64_t the id of the query
 * @return job* the job or NULL
 */
job* create_query_job(job_scheduler*, char*, table_index*, uint64_t);
/**
 * Creates and returns a presort job
 * @param job_query_parameters* the query parameters
 * @param relation** the relation to create and sort if needed
 * @param table_column* the table id and column id
 * @param pthread_mutex_t* mutex used for accessing the unsorted rows counter
 * @param bool if we need to sort the relation
 * @param uint64_t* counter of the unsorted rows
 * @return job* the job or NULL
 */
job* create_presort_job(job_query_parameters*, relation**, table_column*, pthread_mutex_t*, bool, uint64_t*);
/**
 * Creates and returns a sort job
 * @param job_query_parameters* the query parameters
 * @param relation* the relation to sort
 * @param relation* the auxiliary relation
 * @param pthread_mutex_t* mutex used for accessing the unsorted rows counter
 * @param uint64_t* counter of the unsorted rows
 * @param byte the byte used for sorting
 * @param start the starting index to sort the relation
 * @param end the ending index to sort the relation
 * @return job* the job or NULL
 */
job* create_sort_job(job_query_parameters*, relation*, relation*, pthread_mutex_t*, uint64_t*, unsigned short, uint64_t, uint64_t);
/**
 * Creates and returns a join job
 * @param uint64_t staring index of the r relation for the join
 * @param uint64_t ending index of the r relation for the join
 * @param uint64_t staring index of the s relation for the join
 * @param uint64_t* counter of the unjoined parts
 * @param pthread_mutex_t* mutex used to update the counter after the join is finished
 * @param list_array* where the results will be stored
 * @param uint64_t which index to store the results
 * @param job_query_parameters* the query job parameters
 * @return job* the job or NULL
 */
job* create_join_job(uint64_t, uint64_t, uint64_t, uint64_t*, pthread_mutex_t*, list_array*, uint64_t, job_query_parameters*);
/**
 * Creates and returns a join job
 * @param table* table to check the values from
 * @param uint64_t staring index for the filter
 * @param uint64_t ending index for the filter
 * @param uint64_t* counter of the unfiltered parts
 * @param pthread_mutex_t* mutex used to update the counter after the filter is finished
 * @param list_array* where the results will be stored
 * @param uint32_t which index to store the results
 * @param job_query_parameters* the query job parameters
 * @return job* the job or NULL
 */
job* create_filter_table_job(table*, uint64_t,uint64_t,uint64_t*, pthread_mutex_t*, list_array*, uint32_t, job_query_parameters*);
/**
 * Creates and returns a join job
 * @param table* table to check the values from
 * @param middle_list_node* staring node for the filter
 * @param uint32_t number of nodes to filter
 * @param uint64_t* counter of the unfiltered parts
 * @param pthread_mutex_t* mutex used to update the counter after the filter is finished
 * @param list_array* where the results will be stored
 * @param uint32_t which index to store the results
 * @param job_query_parameters* the query job parameters
 * @return job* the job or NULL
 */
job* create_filter_middle_job(table*, middle_list_node*,uint32_t,uint64_t*, pthread_mutex_t*, list_array*, uint32_t, job_query_parameters*);
/**
 * Creates and returns a projection job
 * @param job_query_parameters* the query parameters
 * @param pthread_mutex_t* mutex used for accessing the projections counter
 * @param uint32_t* counter of the projections
 * @param projection* the projection to find
 * @param uint32_t the projection index
 * @return job* the job or NULL
 */
job* create_projection_job(job_query_parameters* p, pthread_mutex_t* mutex, uint64_t* projections_left, projection* pr, uint32_t pr_index);
/**
 * Creates and initializes a job scheduler
 * @param uin32_t thread count
 * @return job_scheduler* the created job scheduler or NULL
 */
job_scheduler* create_job_scheduler(uint32_t);
/**
 * Deletes the job scheduler given
 * @param job_scheduler* the job scheduler to delete
 */
void destroy_job_scheduler(job_scheduler*);
/**
 * Adds a fast job to the scheduler using a mutex
 * @param job_scheduler* the job scheduler to add the job
 * @param job* the job to add
 * @return int 0 if successful
 */
int schedule_fast_job(job_scheduler*, job*);
/**
 * Returns the next job from the scheduler
 * @param job_scheduler* the scheduler
 * @return job* the next job for execution
 */
job* get_job(job_scheduler*);
#if defined(SORTED_PROJECTIONS)
/**
 * Add a projection result to the scheduler
 * @param job_scheduler* The job scheduler
 * @param uint64_t the query id
 * @param uint32_t the number of projections of the query
 * @param uint32_t the projection index
 * @param uint64_t the projection result
 * @return int 0 if successful
 */
int store_projection_in_scheduler(job_scheduler* js, uint64_t query_id, uint32_t number_of_projections, uint32_t projection_index, uint64_t projection_sum);
#endif
#endif /* JOB_SCHEDULER_H */
