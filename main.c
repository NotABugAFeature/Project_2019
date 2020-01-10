#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <inttypes.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include "query.h"
#include "string_list.h"
#include "execute_query.h"
#include "job_scheduler.h"
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

bool th_term=false; //Used to signal the thread termination
/**
 * The thread function
 * @param void* (thread_parameters) The Thread Parameters
 */
void *Thread_Function(void* thr_arg);

//We can use this struct instead of global
//The Parameters To Pass To The Threads 
typedef struct
{
    //    pthread_mutex_t*
    //    sem*
    job_scheduler* jobs;
} thread_parameters;
int main(int argc, char** argv)
{
    //TODO command line args
    //TODO change positions of some parts of the code
    //TODO create the query job and delete the execute code here
    //Initialize The Semaphores
    //TODO destroy sem/mutexes
    uint16_t worker_th=10; //argv
    job_scheduler* scheduler=create_job_scheduler();
    if(scheduler==NULL)
    {
        return 1;
    }
    thread_parameters tp; //If needed
    tp.jobs=scheduler;
    pthread_t threads[worker_th];
    //Start The Threads
    for(int i=0; i<worker_th; i++)
    {
        if((pthread_create(&threads[i], NULL, Thread_Function, &tp)))
        {//ERROR
            fprintf(stderr, "Pthread_create error\n");
            return 2;
        }
    }
    /***********************Will we change the code later**********************/
    string_list *list=read_tables();
    printf("List of names:\n");
    string_list_print(list);
    table_index *ti=insert_tables_from_list(list);
    printf("ti->num_tables: %" PRIu64 "\n", ti->num_tables);
    for(uint32_t i=0; i<ti->num_tables; i++)
    {
        printf("ti->tables[%d].table_id: %" PRIu32 " - ti->tables[%d].columns: %" PRIu64 " - ti->tables[%d].rows: %" PRIu64 "\n", i, ti->tables[i].table_id, i, ti->tables[i].columns, i, ti->tables[i].rows);
    }
    uint64_t queries_count=0;
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
            job* newjob=create_query_job(scheduler,query_str,ti,queries_count);
            if(newjob==NULL)
            {
                break;
            }
//            pthread_mutex_lock(&scheduler->job_fifo_mutex);
            //Append to fifo
            //TODO Add checks
            schedule_job(scheduler,newjob);
//            pthread_mutex_unlock(&job_fifo_mutex);
//            sem_post(&fifo_job_counter_sem);
            queries_count++;
        }
        query_str=string_list_remove(list);
        if(strcmp(query_str, "Done")==0||strcmp(query_str, "done")==0)
        {
            string_list_delete(list);
            break;
        }
        string_list_delete(list);
    }
    /**************************************************************************/
    sleep(100);

    th_term=true;
    for(int i=0;i<worker_th;i++)
    {//Inform All Thread To Exit
        sem_post(&scheduler->fifo_job_counter_sem);
    }
    for(int i=0;i<worker_th;i++)
    {
        if(pthread_join(threads[i], NULL))
        {//ERROR
            fprintf(stderr,"Pthread_join error\n");
        }
        printf("Thread %d Exited\n",i);
    }
    delete_job_scheduler(scheduler);
    delete_table_index(ti);
//    sem_destroy(&fifo_job_counter_sem);
    return 0;
}
void *Thread_Function(void * thr_arg)
{
    //Read the parameters if needed
    thread_parameters* tp=(thread_parameters *) (thr_arg);
    while(true&&!th_term)
    {
        //Pop A Job From The Buffer
//        sem_wait(&fifo_job_counter_sem);
//        if(th_term)
//        {
//            break;
//        }
        //TODO Add checks
//        pthread_mutex_lock(&job_fifo_mutex);
        //Pop job from fifo
        job* j=get_job(tp->jobs);
//        pthread_mutex_unlock(&job_fifo_mutex);
        if(j!=NULL)
        {
            j->run(j->parameters);
        }
        else
        {
            break;
        }
        //Ecexute the job
    }
    pthread_exit(NULL);
}
