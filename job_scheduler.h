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

typedef struct job_scheduler
{
    job_fifo* fifo;
    uint32_t job_count;
    pthread_mutex_t job_fifo_mutex; //Mutex for accessing the job fifo
    sem_t fifo_job_counter_sem;     //Semaphore that counts the items in the job fifo
//    bool clean
//    + something else
} job_scheduler;


typedef struct job
{
    job_scheduler* scheduler;
    int (*run)(void*);
    void (*destroy)(void*);
    void* parameters;
}job;

typedef struct job_query_parameters
{
    uint64_t query_id;
    char* query_str;
    query* query;
    table_index *tables;
    middleman* middle;
    bool* bool_array;
    uint32_t b_counter;
    uint32_t** joined_tables;
    uint32_t pred_index;
    job* this_job;
    pthread_mutex_t r_mutex;
    relation* r;
    pthread_mutex_t s_mutex;
    relation* s;
    uint64_t unsorted_r_rows;
    uint64_t unsorted_s_rows;
}job_query_parameters;
typedef struct job_presort_parameters
{
    relation** r;
    relation* r_s;
    bool sort;
    pthread_mutex_t *mutex;
    uint64_t* unsorted_r_rows;
    query* query;
    table_column* join;
    table_index* tables;
    middleman* middle;
    job* this_job;
}job_presort_parameters;

typedef struct job_sort_parameters
{
    relation* r;
    relation* r_s;
    pthread_mutex_t *mutex;
    uint64_t* unsorted_r_rows;
    job* this_job;
    window win;
}job_sort_parameters;

int run_query_job(void*);
int run_execute_job(void*);
int run_presort_job(void*);
int run_sort_job(void*);
int run_prejoin_job(void*);
int run_join_job(void*);
void destroy_query_job(void*);
job* create_query_job(job_scheduler*, char*, table_index*,uint64_t);
job* create_presort_job(job_query_parameters*,relation**,table_column*,pthread_mutex_t*,bool,uint64_t*);




job_scheduler* create_job_scheduler();
void delete_job_scheduler(job_scheduler*);
int schedule_job(job_scheduler*, job*);
job* get_job(job_scheduler*);
#endif /* JOB_SCHEDULER_H */
