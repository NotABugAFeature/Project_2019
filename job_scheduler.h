#ifndef JOB_SCHEDULER_H
#define JOB_SCHEDULER_H
#include <stdio.h>
#include <inttypes.h>
#include "job_fifo.h"
typedef struct job
{
} job;

void print_job(job*);
void delete_job(job*);
typedef struct job_scheduler
{
    job_fifo fifo;
    uint32_t thread_count;
//    + something else
} job_scheduler;
void print_job_scheduler(job_scheduler*);
void delete_job_scheduler(job_scheduler*);
#endif /* JOB_SCHEDULER_H */
