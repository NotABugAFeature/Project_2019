#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <inttypes.h>
#include <pthread.h>
#include <semaphore.h>
#include <time.h>
#include "query.h"
#include "string_list.h"
#include "execute_query.h"
#include "job_scheduler.h"
#define THREAD_LIMIT 128
#define MAXQUERIES 1024
/**
 * Reads queries from stdin and returns them in a list
 * @return string_list of the queries
 */
string_list *read_batch(void)
{
    char line[STRING_SIZE];
    string_list *list=string_list_create();
    while(1)
    {
        if(fgets(line, STRING_SIZE, stdin)==NULL||feof(stdin))
        {
            printf("End of input\n");
            string_list_delete(list);
            return NULL;
        }
        if(line==NULL)
        {
            string_list_delete(list);
            return NULL;
        }
        if(line[strlen(line)-1]=='\n')
        {
            line[strlen(line)-1]='\0';
            if(line[strlen(line)-1]=='\r')
            {
                line[strlen(line)-1]='\0';
            }
        }
        if(strlen(line)<1)
        {
            continue;
        }
        if(strcmp(line, "F")==0||strcmp(line, "f")==0)
        {
            string_list_insert(list, line);
            break;
        }
        if(strcmp(line, "Done")==0||strcmp(line, "done")==0)
        {
            string_list_insert(list, line);
            break;
        }
        string_list_insert(list, line);
    }
    return list;
}
/**
 * The thread function
 * @param void* (thread_parameters) The Thread Parameters
 */
void *Thread_Function(void* thr_arg);
//We can use this struct instead of global
//The Parameters To Pass To The Threads 
typedef struct
{
    job_scheduler* jobs;
} thread_parameters;
int main(int argc, char** argv)
{
#if defined(MAX_QUERIES_LIMIT)&&!defined(ONE_QUERY_AT_A_TIME)
    if(argc!=3)
    {
        printf("Please give the number of threads to create and the max queries to execute at the same time\n");
#else
    if(argc!=2)
    {
        printf("Please give the number of threads to create\n");
#endif
        return 1;
    }
    uint32_t worker_th=atoi(argv[1]);
    if(worker_th==0)
    {
        printf("The number of threads given is 0\n");
        return 1;
    }
    if(worker_th>THREAD_LIMIT)
    {
        printf("The number of threads given > limit\n");
        return 1;
    }
    printf("The program will create %"PRIu32" threads\n", worker_th);
#if defined(MAX_QUERIES_LIMIT)&&!defined(ONE_QUERY_AT_A_TIME)
    uint32_t max_queries=atoi(argv[2]);
    if(max_queries==0)
    {
        printf("The number of max queries given is 0\n");
        return 1;
    }
    if(max_queries>MAXQUERIES)
    {
        printf("The number of threads given > limit\n");
        return 1;
    }
    printf("The program will run %"PRIu32" queries simultaneously\n", max_queries);
    uint32_t q_max_counter=max_queries;
#endif
    job_scheduler* scheduler=create_job_scheduler(worker_th);
    if(scheduler==NULL)
    {
        return 1;
    }
    thread_parameters tp; //If needed
    tp.jobs=scheduler;
    pthread_t threads[worker_th];
    //Start The Threads
    for(uint32_t i=0; i<worker_th; i++)
    {
        if((pthread_create(&threads[i], NULL, Thread_Function, &tp)))
        {//ERROR
            fprintf(stderr, "Pthread_create error\n");
            if(i==0)
            {
                scheduler->thread_count=i;
            }
            else
            {
                scheduler->thread_count=i-1;
            }
            destroy_job_scheduler(scheduler);
            return 2;
        }
    }
    string_list *list=read_tables();
    struct timespec begin, end;
    clock_gettime(CLOCK_MONOTONIC, &begin);
    printf("List of names:\n");
    string_list_print(list);
    table_index *ti=insert_tables_from_list(list);
    printf("ti->num_tables: %" PRIu64 "\n", ti->num_tables);
    for(uint32_t i=0; i<ti->num_tables; i++)
    {
        printf("ti->tables[%d].table_id: %" PRIu32 " - ti->tables[%d].columns: %" PRIu64 " - ti->tables[%d].rows: %" PRIu64 "\n", i, ti->tables[i].table_id, i, ti->tables[i].columns, i, ti->tables[i].rows);
    }
    uint32_t queries_count=0;
    clock_gettime(CLOCK_MONOTONIC, &end);
    printf("Time to load the tables = %f seconds\n", (end.tv_nsec-begin.tv_nsec)/1000000000.0+(end.tv_sec-begin.tv_sec));
    clock_gettime(CLOCK_MONOTONIC, &begin);
    while(1)
    {
        list=read_batch();
        if(list==NULL)
        {
            break;
        }
        //Call query analysis, execute queries
        char *query_str;
        while(list->num_nodes>1)
        {
            query_str=string_list_remove(list);
            job* newjob=create_query_job(scheduler, query_str, ti, queries_count);
            if(newjob==NULL)
            {
                break;
            }
            //TODO Add checks
            schedule_fast_job(scheduler, newjob);
            queries_count++;
#if defined(MAX_QUERIES_LIMIT)&&!defined(ONE_QUERY_AT_A_TIME)
            if(q_max_counter>0)
            {
                q_max_counter--;
            }
            else
            {
                sem_wait(&scheduler->fifo_query_executing_sem);
            }
#elif defined(ONE_QUERY_AT_A_TIME)
            sem_wait(&scheduler->fifo_query_executing_sem);
#endif
        }
        query_str=string_list_remove(list);
        if(strcmp(query_str, "Done")==0||strcmp(query_str, "done")==0)
        {
            free(query_str);
            string_list_delete(list);
            break;
        }
        free(query_str);
        string_list_delete(list);
    }
#if !defined(ONE_QUERY_AT_A_TIME)
#if defined(MAX_QUERIES_LIMIT)
    if(q_max_counter>0)
    {
        max_queries=queries_count;
    }
    for(uint32_t i=0; i<max_queries; i++)
#else
    for(uint32_t i=0; i<queries_count; i++)
#endif
    {
        sem_wait(&scheduler->fifo_query_executing_sem);
    }
#endif
    clock_gettime(CLOCK_MONOTONIC, &end);
    printf("Total queries: %"PRIu32"\n", queries_count);
    printf("Time to execute all queries = %f seconds\n", (end.tv_nsec-begin.tv_nsec)/1000000000.0+(end.tv_sec-begin.tv_sec));
    printf("Total fast jobs: %"PRIu64"\n", scheduler->fast_job_count);
    destroy_job_scheduler(scheduler);
    delete_table_index(ti);
    for(int i=0; i<worker_th; i++)
    {
        if(pthread_join(threads[i], NULL))
        {//ERROR
            fprintf(stderr, "Pthread_join error\n");
        }
        printf("Thread %d Exited\n", i);
    }
    //TODO add fifo tests to test file
    return 0;
}
void *Thread_Function(void * thr_arg)
{
    //Read the parameters if needed
    thread_parameters* tp=(thread_parameters *) (thr_arg);
    if(tp==NULL||tp->jobs==NULL)
    {
        fprintf(stderr, "Thread parameters error\n");
        pthread_exit(NULL);
    }
    while(true)
    {
        //Pop job from the scheduler's fifo
        job* j=get_job(tp->jobs);
        if(j!=NULL)
        {
            //Execute the job
            if(j->run(j->parameters))
            {
                fprintf(stderr, "Thread run error\n");
                break;
            }
        }
        else
        {
            break;
        }
    }
    sem_post(&tp->jobs->threads_finished_sem);
    pthread_exit(NULL);
}
