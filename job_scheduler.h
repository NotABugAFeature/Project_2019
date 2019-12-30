#ifndef JOB_SCHEDULER_H
#define JOB_SCHEDULER_H
#include <stdio.h>
#include <inttypes.h>
#include "job_fifo.h"
#include "table.h"
typedef struct job
{
    int (*run)(void*);
    void (*print)(void*);
    void (*destroy)(void*);
    void* parameters;
} job;

typedef struct job_query_parameters
{
    char* query_str;
    table_index *tables;
    uint64_t query_id;
}job_query_parameters;
int run_query_job(void*);
void print_query_job(void*);
void destroy_query_job(void*);
job* create_query_job(char*, table_index*,uint64_t);
typedef struct job_scheduler
{
    job_fifo* fifo;
    uint32_t job_count;
//    bool clean
//    + something else
} job_scheduler;
job_scheduler* create_job_scheduler();
void delete_job_scheduler(job_scheduler*);
int schedule_job(job_scheduler*, job*);
job* get_job(job_scheduler*);
#endif /* JOB_SCHEDULER_H */
