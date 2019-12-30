#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <inttypes.h>
#include <pthread.h>
#include <semaphore.h>
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
            break;
        }
        string_list_insert(list, line);
    }
    return list;
}


//Mutex for accessing the job fifo
pthread_mutex_t job_fifo_mutex=PTHREAD_MUTEX_INITIALIZER;
//Semaphore that counts the items in the job fifo
sem_t fifo_job_counter_sem;
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
}thread_parameters;

int main(int argc,char** argv)
{
    //TODO command line args
    //TODO change positions of some parts of the code
    //TODO create the query job and delete the execute code here
    //Initialize The Semaphores
    if(sem_init(&fifo_job_counter_sem, 0, 0)!=0)
    {
        perror("Semaphore fifo_job_counter_sem initialization");
        return 1;
    }
    uint16_t worker_th=10;//argv
    thread_parameters tp;//If needed
    pthread_t threads[worker_th];
    //Start The Threads
    for(int i=0;i<worker_th;i++)
    {
        if((pthread_create(&threads[i], NULL, Thread_Function, &tp)))
        {//ERROR
            fprintf(stderr,"Pthread_create error\n");
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
    while(1)
    {
        list=read_batch();
        if(list==NULL)
        {
            break;
        }
        //Call query analysis, execute queries
        char *query_str;
        while(list->num_nodes>0)
        {
            query_str=string_list_remove(list);
            printf("The query to analyze: %s\n", query_str);
            query* q=create_query();
            if(q==NULL)
            {
                delete_table_index(ti);
                free(query_str);
                string_list_delete(list);
                return -3;
            }
            if(analyze_query(query_str, q)!=0)
            {
                delete_query(q);
                free(query_str);
                continue;
            }
            free(query_str);
            //printf("After analyzing: ");
            //print_query_like_an_str(q);
            if(validate_query(q, ti)!=0)
            {
                delete_query(q);
                continue;
            }
            //printf("After validation: ");
            //print_query_like_an_str(q);
            if(optimize_query(q, ti)!=0)
            {
                delete_query(q);
                continue;
            }
            //printf("After optimizing: ");
            //print_query_like_an_str(q);
            bool* bool_array=NULL;
            if(create_sort_array(q, &bool_array)!=0)
            {
                delete_query(q);
                continue;
            }
            //printf("After creating bool array: ");
            //print_query_like_an_str(q);
            if(optimize_query_memory(q)!=0)
            {
                delete_query(q);
                continue;
            }
            printf("After optimizing memory: ");
            print_query_like_an_str(q);
            /*
            for(uint32_t i=0; i<q->number_of_predicates*2; i++)
            {
                printf("%d", bool_array[i]);
            }
                printf("\n");
             */
            //Execute
            middleman *middle=execute_query(q, ti, bool_array);
            calculate_projections(q, ti, middle);
            //Free memory
            for(uint32_t i=0; i<middle->number_of_tables; i++)
            {
                if(middle->tables[i].list!=NULL)
                    delete_middle_list(middle->tables[i].list);
            }
            free(middle->tables);
            free(middle);
            free(bool_array);
            delete_query(q);
        }
        string_list_delete(list);
    }
    /**************************************************************************/
    th_term=true;
    for(int i=0;i<worker_th;i++)
    {
        th_term=true;
        for(int i=0;i<worker_th;i++)
        {//Inform All Thread To Exit
            sem_post(&fifo_job_counter_sem);
        }
        if(pthread_join(threads[i], NULL))
        {//ERROR
            fprintf(stderr,"Pthread_join error\n");
        }
        printf("Thread %d Exited",i);
    }
    delete_table_index(ti);
    return 0;
}


void *Thread_Function(void * thr_arg)
{
    //Read the parameters if needed
    thread_parameters* tp=(thread_parameters *)(thr_arg);
    while(true&&!th_term)
    {
        //Pop A Job From The Buffer
        sem_wait(&fifo_job_counter_sem);
        if(th_term)
        {
            break;
        }
        pthread_mutex_lock(&job_fifo_mutex);
        //Pop job from fifo
        pthread_mutex_unlock(&job_fifo_mutex);
        //Ecexute the job
    }
    pthread_exit(NULL);
}
