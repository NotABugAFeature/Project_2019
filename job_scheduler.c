#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "job_scheduler.h"
#include "sort_merge_join.h"
#include "execute_query.h"
job_scheduler* create_job_scheduler(uint32_t thread_count)
{
    if(thread_count==0)
    {
        fprintf(stderr, "schedule_job: 0 thread_count\n");
        return NULL;
    }
    job_scheduler* js=malloc(sizeof(job_scheduler));
    if(js==NULL)
    {
        perror("create_job_scheduler: malloc error");
        return NULL;
    }
    //Initialize the semaphores/mutexes
    if(sem_init(&js->fifo_job_counter_sem, 0, 0)!=0)
    {
        perror("create_job_scheduler fifo_job_counter_sem initialization");
        free(js);
        return NULL;
    }
    if(sem_init(&js->fifo_query_executing_sem, 0, 0)!=0)
    {
        perror("create_job_scheduler fifo_query_executing_sem initialization");
        sem_destroy(&js->fifo_job_counter_sem);
        free(js);
        return NULL;
    }
    if(sem_init(&js->threads_finished_sem, 0, 0)!=0)
    {
        perror("create_job_scheduler threads_finished_sem initialization");
        sem_destroy(&js->fifo_job_counter_sem);
        sem_destroy(&js->fifo_query_executing_sem);
        free(js);
        return NULL;
    }
    if(pthread_mutex_init(&js->job_fifo_mutex, NULL)!=0)
    {
        perror("create_job_scheduler fifo_job_mutex initialization");
        sem_destroy(&js->fifo_job_counter_sem);
        sem_destroy(&js->fifo_query_executing_sem);
        sem_destroy(&js->threads_finished_sem);
        free(js);
        return NULL;
    }
#if defined(SORTED_PROJECTIONS)
    //Create the projection list
    js->projection_list=create_projection_list();
    if(js->projection_list==NULL)
    {
        pthread_mutex_destroy(&js->job_fifo_mutex);
        sem_destroy(&js->fifo_job_counter_sem);
        sem_destroy(&js->fifo_query_executing_sem);
        sem_destroy(&js->threads_finished_sem);
        free(js);
        return NULL;
    }
#endif
    //Create the job fifo
    js->fast_fifo=create_job_fifo();
    if(js->fast_fifo==NULL)
    {
#if defined(SORTED_PROJECTIONS)
        delete_projection_list(js->projection_list);
#endif
        pthread_mutex_destroy(&js->job_fifo_mutex);
        sem_destroy(&js->fifo_job_counter_sem);
        sem_destroy(&js->fifo_query_executing_sem);
        sem_destroy(&js->threads_finished_sem);
        free(js);
        return NULL;
    }
    js->fast_job_count=0;
    js->thread_count=thread_count;
    js->terminate=false;
    return js;
}
void destroy_job_scheduler(job_scheduler* js)
{
    if(js==NULL)
    {
        return;
    }
    js->terminate=true;
    for(int i=0; i<js->thread_count; i++)
    {//Inform All Thread To Exit
        sem_post(&js->fifo_job_counter_sem);
    }
    for(int i=0; i<js->thread_count; i++)
    {//Wait for all the treads to exit
        sem_wait(&js->threads_finished_sem);
    }
    delete_job_fifo(js->fast_fifo);
#if defined(SORTED_PROJECTIONS)
    //Delete the projection list
    delete_projection_list(js->projection_list);
#endif
    //Destroy the semaphores/mutexes
    pthread_mutex_destroy(&js->job_fifo_mutex);
    sem_destroy(&js->fifo_job_counter_sem);
    sem_destroy(&js->fifo_query_executing_sem);
    sem_destroy(&js->threads_finished_sem);
    free(js);
    js=NULL;
}
int schedule_fast_job(job_scheduler* js, job* j)
{
    if(js==NULL||js->fast_fifo==NULL||j==NULL)
    {
        fprintf(stderr, "schedule_job: NULL parameter\n");
        return -1;
    }
    //Aquire lock
    pthread_mutex_lock(&js->job_fifo_mutex);
    //Add it to the fifo
    if(append_to_job_fifo(js->fast_fifo, j)==0)
    {
        js->fast_job_count++;
        //Release lock
        pthread_mutex_unlock(&js->job_fifo_mutex);
        //Inform the threads waiting for jobs
        sem_post(&js->fifo_job_counter_sem);
        return 0;
    }
    //Release lock
    pthread_mutex_unlock(&js->job_fifo_mutex);
    return -2;
}
#if defined(SORTED_PROJECTIONS)
int store_projection_in_scheduler(job_scheduler* js, uint64_t query_id, uint32_t number_of_projections, uint32_t projection_index, uint64_t projection_sum)
{
    if(js==NULL||js->projection_list==NULL||projection_index>number_of_projections)
    {
        fprintf(stderr, "schedule_projection: wrong parameters\n");
        return -1;
    }
    //Aquire lock
    pthread_mutex_lock(&js->projection_list->mutex);
    //Append to list
    if(append_to_projection_list(js->projection_list, query_id, number_of_projections, projection_index, projection_sum)==0)
    {
        //Release lock
        pthread_mutex_unlock(&js->projection_list->mutex);
        return 0;
    }
    //Release lock
    pthread_mutex_unlock(&js->projection_list->mutex);
    return -2;
}
#endif
job* get_job(job_scheduler* js)
{
    if(js==NULL||js->fast_fifo==NULL)
    {
        fprintf(stderr, "get_job: NULL parameter\n");
        return NULL;
    }
    //Wait for a job to arrive
    sem_wait(&js->fifo_job_counter_sem);
    //Aquire lock
    pthread_mutex_lock(&js->job_fifo_mutex);
    //Pop from fifo
    if(js->terminate)
    {
        //Release the lock
        pthread_mutex_unlock(&js->job_fifo_mutex);
        return NULL;
    }
    else
    {
        job* j=pop_from_job_fifo(js->fast_fifo);
        //Release the lock
        pthread_mutex_unlock(&js->job_fifo_mutex);
        return j;
    }
}
job* create_query_job(job_scheduler* jb, char* query_str, table_index* ti, uint64_t id)
{
    if(jb==NULL||query_str==NULL||ti==NULL)
    {
        fprintf(stderr, "create_query_job: NULL parameter\n");
        return NULL;
    }
    //Initialize the job and its parameters
    job* newjob=malloc(sizeof(job));
    if(newjob==NULL)
    {
        perror("create_query_job: malloc error");
        return NULL;
    }
    job_query_parameters* par=malloc(sizeof(job_query_parameters));
    if(par==NULL)
    {
        free(newjob);
        return NULL;
    }
    if(pthread_mutex_init(&par->r_mutex, NULL))
    {
        fprintf(stderr, "create_query_job: pthread_mutex_init error\n");
        free(par);
        free(newjob);
        return NULL;
    }
    if(pthread_mutex_init(&par->s_mutex, NULL))
    {
        fprintf(stderr, "create_query_job: pthread_mutex_init error\n");
        pthread_mutex_destroy(&par->r_mutex);
        free(par);
        free(newjob);
        return NULL;
    }
    if(pthread_mutex_init(&par->q_mutex, NULL))
    {
        fprintf(stderr, "create_query_job: pthread_mutex_init error\n");
        pthread_mutex_destroy(&par->r_mutex);
        pthread_mutex_destroy(&par->s_mutex);
        free(par);
        free(newjob);
        return NULL;
    }
    par->is_prejoin_scheduled=false;
    par->query_str=query_str;
    par->tables=ti;
    par->query_id=id;
    par->middle=NULL;
    par->bool_array=NULL;
    par->query=NULL;
    par->b_index=0;
    par->joined_tables=NULL;
    par->pred_index=0;
    par->this_job=newjob;
    par->r=NULL;
    par->s=NULL;
    par->r_counter=0;
    par->s_counter=0;
    newjob->scheduler=jb;
    newjob->parameters=(void*) par;
    newjob->run=&run_query_job;
    newjob->destroy=&destroy_query_job;
    return newjob;
}
int run_query_job(void* parameters)
{
    job_query_parameters* p=(job_query_parameters*) parameters;
    if(p==NULL||p->query_str==NULL)
    {
        fprintf(stderr, "run_query_job: NULL parameters\n");
        return -1;
    }
    if(p->tables==NULL||p->bool_array!=NULL||p->middle!=NULL||
       p->query!=NULL)
    {
        destroy_query_job(parameters);
        fprintf(stderr, "run_query_job: wrong parameters\n");
        return -2;
    }
    //    printf("The query to analyze: %s\n",p->query_str);
    p->query=create_query();
    if(p->query==NULL)
    {
        destroy_query_job(parameters);
        return -3;
    }
    if(analyze_query(p->query_str, p->query)!=0)
    {
        destroy_query_job(parameters);
        return 0;
    }
    free(p->query_str);
    p->query_str=NULL;
    if(validate_query(p->query, p->tables)!=0)
    {
        destroy_query_job(parameters);
        return 0;
    }
    if(optimize_query(p->query, p->tables)!=0)
    {
        destroy_query_job(parameters);
        return 0;
    }
    if(create_sort_array(p->query, &p->bool_array)!=0)
    {
        destroy_query_job(parameters);
        return 0;
    }
    optimize_query_memory(p->query);
    //    printf("After optimizing memory: ");
    //    print_query_like_an_str(q);
    //Change the execution function
    p->this_job->run=&run_execute_job;
    if(p->this_job->run(parameters)<0)
    {
        return -4;
    }
    return 0;
}
int run_execute_job(void* parameters)
{
    job_query_parameters* p=(job_query_parameters*) parameters;
    if(p==NULL||p->query_str!=NULL||p->bool_array==NULL||p->query==NULL||p->tables==NULL||p->this_job==NULL)
    {
        fprintf(stderr, "run_execute_job: wrong parameters\n");
        destroy_query_job(parameters);
        return -1;
    }
    int result=execute_query_parallel(p);
    if(result!=0)
    {
        fprintf(stderr, "run_execute_job: execute_query_parallel error\n");
        destroy_query_job(parameters);
        return -1;
    }
    else if(result==0)
    {
#if defined(SERIAL_EXECUTION)
    destroy_query_job(parameters);
#endif
    }
    return 0;
}
void destroy_query_job(void* parameters)
{
    //Free the resources
    job_query_parameters* p=(job_query_parameters*) parameters;
    if(p==NULL)
    {
        return;
    }
    if(p->query_str!=NULL)
    {
        free(p->query_str);
        p->query_str=NULL;
    }
    if(p->bool_array!=NULL)
    {
        free(p->bool_array);
        p->bool_array=NULL;
    }
    if(p->joined_tables!=NULL&&p->query!=NULL)
    {
        for(uint32_t i=0; i<p->query->number_of_predicates; i++)
        {
            free((p->joined_tables)[i]);
        }
        free(p->joined_tables);
        p->joined_tables=NULL;
    }
    if(p->middle!=NULL)
    {
        for(uint32_t i=0; i<p->middle->number_of_tables; i++)
        {
            if(p->middle->tables[i].list!=NULL)
            {
                delete_middle_list(p->middle->tables[i].list);
            }
        }
        free(p->middle->tables);
        free(p->middle);
    }
    if(p->query!=NULL)
    {
        delete_query(p->query);
        p->query=NULL;
    }
    if(p->r!=NULL)
    {
        if(p->r->tuples!=NULL)
        {
            free(p->r->tuples);
            p->r->tuples=NULL;
        }
        free(p->r);
        p->r=NULL;
    }
    if(p->s!=NULL)
    {
        if(p->s->tuples!=NULL)
        {
            free(p->s->tuples);
            p->s->tuples=NULL;
        }
        free(p->s);
        p->s=NULL;
    }
    pthread_mutex_destroy(&p->r_mutex);
    pthread_mutex_destroy(&p->s_mutex);
    sem_post(&p->this_job->scheduler->fifo_query_executing_sem);
    if(p->this_job!=NULL)
    {
        free(p->this_job);
        p->this_job=NULL;
    }
    free(p);
    p=NULL;
}
void destroy_presort_job(void* parameters)
{
    job_presort_parameters* p=(job_presort_parameters*) parameters;
    free(p->this_job);
    free(parameters);
}
void destroy_sort_job(void* parameters)
{
    job_sort_parameters* p=(job_sort_parameters*) parameters;
    free(p->this_job);
    free(parameters);
}
void destroy_projection_job(void* parameters)
{
    job_projection_parameters* p=(job_projection_parameters*) parameters;
    free(p->this_job);
    p->this_job=NULL;
    p->mutex=NULL;
    p->projection=NULL;
    p->projections_left=NULL;
    p->query_parameters=NULL;
    free(parameters);
}
int run_prejoin_job(void* parameters)
{
    job_query_parameters* p=(job_query_parameters*) parameters;
    if(p==NULL||p->query_str!=NULL||p->bool_array==NULL||p->query==NULL||p->tables==NULL||p->this_job==NULL||
       p->b_index==0||p->joined_tables==NULL||p->middle==NULL)
    {
        fprintf(stderr, "run_prejoin_job: wrong parameters\n");
        destroy_query_job(parameters);
    }
    //Check if the sorting is complete
    if(p->r_counter!=0||p->s_counter!=0)
    {
        fprintf(stderr, "run_prejoin_job: sorting not completed\n");
        destroy_query_job(parameters);
        return -1;
    }
    p->is_prejoin_scheduled=false;
#if defined(SERIAL_JOIN)
        //Do the join
        predicate_join *join=p->query->predicates[p->pred_index].p;
        int delete_r=1, delete_s=1;
        //Create middle lists in which we are going to store the join results
        middle_list *result_R=create_middle_list();
        if(result_R==NULL)
        {
            fprintf(stderr, "run_join_job: Error in create_result_list\n");
            return -3;
        }
        middle_list *result_S=create_middle_list();
        if(result_S==NULL)
        {
            fprintf(stderr, "run_join_job: Error in create_result_list\n");
            return -3;
        }
        //B.3.4 Join
        if(p->r==NULL&&p->s==NULL)
        {
            return -4;
        }
        if(final_join(result_R, result_S, p->r, p->s))
        {
            fprintf(stderr, "run_join_job: Error in final_join\n");
            return -6;
        }
        //B.3.5 Now go back to middleman
        //If the list exists then update it
        //If not then put the result list (result_R, result_S) in its place
        if(p->middle->tables[join->r.table_id].list==NULL)
        {
            p->middle->tables[join->r.table_id].list=result_R;
            delete_r=0;
        }
        else
        {
            middle_list *new_list=create_middle_list();
            if(result_R->number_of_nodes>0)
            {
                //Construct lookup table of the existing (old) list
#if (defined(SERIAL_JOIN)&&defined(SERIAL_FILTER)&&defined(SERIAL_SELFJOIN))
                middle_list_bucket **lookup=construct_lookup_table(p->middle->tables[join->r.table_id].list);
#else
                lookup_table *lookup=construct_lookup_table(p->middle->tables[join->r.table_id].list);
#endif
                //Traverse result list and for every rowId find it in the old list and put
                //the result in the new_list
                middle_list_node *list_temp=result_R->head;
                while(list_temp!=NULL)
                {
                    if(update_middle_bucket(lookup, &(list_temp->bucket), new_list))
                    {
                        fprintf(stderr, "run_join_job: Error in update_middle_bucket\n");
                        return -7;
                    }
                    list_temp=list_temp->next;
                }
#if (defined(SERIAL_JOIN)&&defined(SERIAL_FILTER)&&defined(SERIAL_SELFJOIN))
                free(lookup);
#else
                delete_lookup_table(lookup);
#endif
            }
            delete_middle_list(p->middle->tables[join->r.table_id].list);
            p->middle->tables[join->r.table_id].list=new_list;
        }
        //Same procedure for S as above
        if(p->middle->tables[join->s.table_id].list==NULL)
        {
            p->middle->tables[join->s.table_id].list=result_S;
            delete_s=0;
        }
        else
        {
            middle_list *new_list=create_middle_list();
            if(result_S->number_of_nodes>0)
            {
#if (defined(SERIAL_JOIN)&&defined(SERIAL_FILTER)&&defined(SERIAL_SELFJOIN))
                middle_list_bucket **lookup=construct_lookup_table(p->middle->tables[join->s.table_id].list);
#else
                lookup_table *lookup=construct_lookup_table(p->middle->tables[join->s.table_id].list);
#endif
                middle_list_node *list_temp=result_S->head;
                while(list_temp!=NULL)
                {
                    if(update_middle_bucket(lookup, &(list_temp->bucket), new_list))
                    {
                        fprintf(stderr, "run_join_job: Error in update_middle_bucket\n");
                        return -8;
                    }
                    list_temp=list_temp->next;
                }
#if (defined(SERIAL_JOIN)&&defined(SERIAL_FILTER)&&defined(SERIAL_SELFJOIN))
                free(lookup);
#else
                delete_lookup_table(lookup);
#endif
            }
            delete_middle_list(p->middle->tables[join->s.table_id].list);
            p->middle->tables[join->s.table_id].list=new_list;
        }
        //B.3.6 Free relations
        if(p->r->num_tuples>0)
        {
            free(p->r->tuples);
            p->r->tuples=NULL;
        }
        free(p->r);
        p->r=NULL;
        if(p->s->num_tuples>0)
        {
            free(p->s->tuples);
            p->s->tuples=NULL;
        }
        free(p->s);
        p->s=NULL;
        //TODO Add if for return !=0
        update_related_lists(p->pred_index, p->query, p->joined_tables, p->middle, delete_r, delete_s, result_R, result_S, NULL);
        p->pred_index++;
        p->this_job->run=run_execute_job;
        schedule_fast_job(p->this_job->scheduler, p->this_job);
        return 0;
#else
    //TODO Figure out size of parts
    uint64_t parts;
    if(p->r->num_tuples<JOIN_TUPLES)
    {
        parts=1;
    }
    else
    {
        parts=p->r->num_tuples/JOIN_TUPLES+1;
    }
    uint64_t small_size=p->r->num_tuples/parts;
    uint64_t extra=p->r->num_tuples%parts;
    p->r_counter=parts;
    //Create a list_array to keep the results of each part
    list_array *la=create_list_array(parts, 2);
    if(la==NULL)
    {
        fprintf(stderr, "run_prejoin_job: error in create_list_array\n");
        return -1;
    }
    //If a relation is empty, no need to split it in parts
    if(p->r->num_tuples==0||p->s->num_tuples==0)
    {
        p->r_counter=1;
        job *newjob=create_join_job(0, 0, 0, &p->r_counter, &p->r_mutex, la, 0, p);
        schedule_fast_job(p->this_job->scheduler, newjob);
        return 0;
    }
    if(parts==1)
    {
        p->r_counter=1;
        job *newjob=create_join_job(0, p->r->num_tuples, 0, &p->r_counter, &p->r_mutex, la, 0, p);
        schedule_fast_job(p->this_job->scheduler, newjob);
        return 0;
    }
    uint64_t start_r, end_r=0, start_s=0;
    for(uint64_t i=0; i<parts; i++)
    {
        start_r=end_r;
        end_r=start_r+small_size;
        if(i<extra)
        {
            end_r++;
        }
        while(start_s<(p->s->num_tuples-1)&&p->s->tuples[start_s].key<p->r->tuples[start_r].key)
        {
            start_s++;
        }
        //Create and schedule the join job
        job *newjob=create_join_job(start_r, end_r, start_s, &p->r_counter, &p->r_mutex, la, i, p);
        schedule_fast_job(p->this_job->scheduler, newjob);
    }
    return 0;
#endif
}
job* create_join_job(uint64_t start_r, uint64_t end_r, uint64_t start_s, uint64_t *unjoined_parts, pthread_mutex_t *parts_mutex, list_array *lists, uint64_t list_position, job_query_parameters *exe_params)
{
    if(start_r>end_r||unjoined_parts==NULL||parts_mutex==NULL||exe_params==NULL)
    {
        fprintf(stderr, "create_join_job: wrong parameters\n");
        return NULL;
    }
    job* newjob=malloc(sizeof(job));
    if(newjob==NULL)
    {
        perror("create_join_job: malloc error");
        return NULL;
    }
    job_join_parameters* par=malloc(sizeof(job_join_parameters));
    if(par==NULL)
    {
        perror("create_join_job: malloc error");
        free(newjob);
        return NULL;
    }
    par->start_r=start_r;
    par->end_r=end_r;
    par->start_s=start_s;
    par->unjoined_parts=unjoined_parts;
    par->parts_mutex=parts_mutex;
    par->list_position=list_position;
    par->lists=lists;
    par->exe_params=exe_params;
    par->this_job=newjob;
    newjob->scheduler=exe_params->this_job->scheduler;
    newjob->parameters=(void *) par;
    newjob->run= &run_join_job;
    newjob->destroy= &destroy_join_job;
    return newjob;
}
job* create_filter_table_job(table* t, uint64_t start_index, uint64_t end_index, uint64_t*unfiltered_parts,
                             pthread_mutex_t* parts_mutex, list_array* la,uint32_t list_pos,job_query_parameters*exe_params)
{
    if(start_index>end_index||unfiltered_parts==NULL||parts_mutex==NULL||exe_params==NULL||
       t==NULL||la==NULL||exe_params==NULL)
    {
        fprintf(stderr, "create_filter_table_job: wrong parameters\n");
        return NULL;
    }
    job* newjob=malloc(sizeof(job));
    if(newjob==NULL)
    {
        perror("create_filter_table_job: malloc error");
        return NULL;
    }
    job_filter_table_parameters* par=malloc(sizeof(job_filter_table_parameters));
    if(par==NULL)
    {
        perror("create_filter_table_job: malloc error");
        free(newjob);
        return NULL;
    }
    par->table=t;
    par->start_index=start_index;
    par->end_index=end_index;
    par->unfiltered_parts=unfiltered_parts;
    par->parts_mutex=parts_mutex;
    par->lists=la;
    par->list_position=list_pos;
    par->exe_params=exe_params;
    par->this_job=newjob;
    newjob->scheduler=exe_params->this_job->scheduler;
    newjob->parameters=(void *) par;
    newjob->run= &run_filter_table_job;
    newjob->destroy= &destroy_filter_table_job;
    return newjob;
}
int run_filter_table_job(void * parameters)
{
    job_filter_table_parameters* p=(job_filter_table_parameters*) parameters;
    if(p==NULL||p->unfiltered_parts==NULL||p->this_job==NULL||p->table==NULL||p->parts_mutex==NULL||p->lists==NULL||p->exe_params==NULL)
    {
        fprintf(stderr, "run_filter_table_job: wrong parameters\n");
        destroy_filter_table_job(parameters);
        return -1;
    }
    predicate_filter *filter=p->exe_params->query->predicates[p->exe_params->pred_index].p;
    middle_list *result=p->lists->lists[p->list_position][0];
    if(filter_original_table_parallel(filter, p->table, p->start_index,p->end_index ,result))
    {
        fprintf(stderr, "run_filter_table_job filter_original_table: Error\n");
        destroy_filter_table_job(parameters);
        return -4;
    }
    pthread_mutex_lock(p->parts_mutex);
    (*(p->unfiltered_parts))--;
    if(*(p->unfiltered_parts)==0)
    {
        pthread_mutex_unlock(p->parts_mutex);
        //Merge the lists of all filters
        middle_list* filter_result=create_middle_list();
        if(filter_result==NULL)
        {
            fprintf(stderr, "run_filter_table_job: Error in create_result_list\n");
            destroy_filter_table_job(parameters);
            return -3;
        }
        merge_middle_list(p->lists, filter_result);
        p->exe_params->middle->tables[filter->r.table_id].list=filter_result;
        //Keep only the execute_parameters (with the old job inside)
        job_query_parameters *exe_params=p->exe_params;
        exe_params->pred_index++;
        exe_params->this_job->run=run_execute_job;
        schedule_fast_job(exe_params->this_job->scheduler, exe_params->this_job);
        delete_list_array(p->lists);
        //        free(p->lists->lists);
        //        free(p->lists);
        //        p->lists=NULL;
        destroy_filter_table_job(parameters);
    }
    else
    {
        pthread_mutex_unlock(p->parts_mutex);
        destroy_filter_table_job(parameters);
    }
    return 0;
}
void destroy_filter_table_job(void * parameters)
{
    job_filter_table_parameters* p=(job_filter_table_parameters*) parameters;
    if(p==NULL)
    {
        return;
    }
    free(p->this_job);
    free(p);
}
job* create_filter_middle_job(table* t, middle_list_node*start_node, uint32_t node_count, uint64_t*unfiltered_parts,
                              pthread_mutex_t* parts_mutex, list_array* la,uint32_t list_pos ,job_query_parameters*exe_params)
{
    if(start_node==NULL||node_count==0||unfiltered_parts==NULL||parts_mutex==NULL||exe_params==NULL||
       t==NULL||la==NULL||exe_params==NULL)
    {
        fprintf(stderr, "create_filter_table_job: wrong parameters\n");
        return NULL;
    }
    job* newjob=malloc(sizeof(job));
    if(newjob==NULL)
    {
        perror("create_filter_table_job: malloc error");
        return NULL;
    }
    job_filter_middle_parameters* par=malloc(sizeof(job_filter_middle_parameters));
    if(par==NULL)
    {
        perror("create_filter_table_job: malloc error");
        free(newjob);
        return NULL;
    }
    par->table=t;
    par->start_node=start_node;
    par->node_count=node_count;
    par->unfiltered_parts=unfiltered_parts;
    par->parts_mutex=parts_mutex;
    par->lists=la;
    par->list_position=list_pos;
    par->exe_params=exe_params;
    par->this_job=newjob;
    newjob->scheduler=exe_params->this_job->scheduler;
    newjob->parameters=(void *) par;
    newjob->run= &run_filter_middle_job;
    newjob->destroy= &destroy_filter_middle_job;
    return newjob;
}
int run_filter_middle_job(void * parameters)
{
    job_filter_middle_parameters* p=(job_filter_middle_parameters*) parameters;
    if(p==NULL||p->unfiltered_parts==NULL||p->this_job==NULL||p->table==NULL||p->parts_mutex==NULL||p->lists==NULL||p->exe_params==NULL||p->start_node==NULL)
    {
        fprintf(stderr, "run_filter_middle_job: wrong parameters\n");
        run_filter_middle_job(parameters);
        return -1;
    }
    predicate_filter *filter=p->exe_params->query->predicates[p->exe_params->pred_index].p;
    middle_list *result=p->lists->lists[p->list_position][0];
    //traverse existing list and store the new results in 'new_list'
    middle_list_node *list_temp=p->start_node;
    while(list_temp!=NULL&&p->node_count>0)
    {
        if(filter_middle_bucket(filter, &(list_temp->bucket), p->table, result))
        {
            fprintf(stderr, "execute_query filter_middle_bucket: Error\n");
            return -4;
        }
        list_temp=list_temp->next;
        p->node_count--;
    }
    pthread_mutex_lock(p->parts_mutex);
    (*(p->unfiltered_parts))--;
    if(*(p->unfiltered_parts)==0)
    {
        pthread_mutex_unlock(p->parts_mutex);
        //Merge the lists of all filters
        middle_list* filter_result=create_middle_list();
        if(filter_result==NULL)
        {
            fprintf(stderr, "run_filter_table_job: Error in create_result_list\n");
            destroy_filter_table_job(parameters);
            return -3;
        }
        merge_middle_list(p->lists, filter_result);
        //delete old list and put the new one in its place
        delete_middle_list(p->exe_params->middle->tables[filter->r.table_id].list);
        p->exe_params->middle->tables[filter->r.table_id].list=filter_result;
        //Keep only the execute_parameters (with the old job inside)
        job_query_parameters *exe_params=p->exe_params;
        exe_params->pred_index++;
        exe_params->this_job->run=run_execute_job;
        schedule_fast_job(exe_params->this_job->scheduler, exe_params->this_job);
        delete_list_array(p->lists);
        //        free(p->lists->lists);
        //        free(p->lists);
        //        p->lists=NULL;
        destroy_filter_table_job(parameters);
    }
    else
    {
        pthread_mutex_unlock(p->parts_mutex);
        destroy_filter_table_job(parameters);
    }
    return 0;
}
void destroy_filter_middle_job(void * parameters)
{
    job_filter_middle_parameters* p=(job_filter_middle_parameters*) parameters;
    if(p==NULL)
    {
        return;
    }
    free(p->this_job);
    free(p);
}
int run_original_self_join_table_job(void * parameters)
{
    job_filter_table_parameters* p=(job_filter_table_parameters*) parameters;
    if(p==NULL||p->unfiltered_parts==NULL||p->this_job==NULL||p->table==NULL||p->parts_mutex==NULL||p->lists==NULL||p->exe_params==NULL)
    {
        fprintf(stderr, "run_filter_table_job: wrong parameters\n");
        destroy_filter_table_job(parameters);
        return -1;
    }
    predicate_join *selfjoin=p->exe_params->query->predicates[p->exe_params->pred_index].p;
    middle_list *result=p->lists->lists[p->list_position][0];
    if(self_join_table_parallel(selfjoin, p->table, p->start_index,p->end_index ,result))
    {
        fprintf(stderr, "run_filter_table_job filter_original_table: Error\n");
        destroy_filter_table_job(parameters);
        return -4;
    }
    pthread_mutex_lock(p->parts_mutex);
    (*(p->unfiltered_parts))--;
    if(*(p->unfiltered_parts)==0)
    {
        pthread_mutex_unlock(p->parts_mutex);
        //Merge the lists of all filters
        middle_list* self_join_result=create_middle_list();
        if(self_join_result==NULL)
        {
            fprintf(stderr, "run_filter_table_job: Error in create_result_list\n");
            destroy_filter_table_job(parameters);
            return -3;
        }
        merge_middle_list(p->lists, self_join_result);
        p->exe_params->middle->tables[selfjoin->r.table_id].list=self_join_result;
        //Keep only the execute_parameters (with the old job inside)
        job_query_parameters *exe_params=p->exe_params;
        exe_params->pred_index++;
        exe_params->this_job->run=run_execute_job;
        schedule_fast_job(exe_params->this_job->scheduler, exe_params->this_job);
        delete_list_array(p->lists);
        //        free(p->lists->lists);
        //        free(p->lists);
        //        p->lists=NULL;
        destroy_filter_table_job(parameters);
    }
    else
    {
        pthread_mutex_unlock(p->parts_mutex);
        destroy_filter_table_job(parameters);
    }
    return 0;
}
int run_original_self_join_middle_job(void * parameters)
{
    job_filter_middle_parameters* p=(job_filter_middle_parameters*) parameters;
    if(p==NULL||p->unfiltered_parts==NULL||p->this_job==NULL||p->table==NULL||p->parts_mutex==NULL||p->lists==NULL||p->exe_params==NULL||p->start_node==NULL)
    {
        fprintf(stderr, "run_filter_middle_job: wrong parameters\n");
        run_filter_middle_job(parameters);
        return -1;
    }
    predicate_join *selfjoin=p->exe_params->query->predicates[p->exe_params->pred_index].p;
    middle_list *result=p->lists->lists[p->list_position][0];
    //traverse existing list and store the new results in 'new_list'
    middle_list_node *list_temp=p->start_node;
    while(list_temp!=NULL&&p->node_count>0)
    {
        if(original_self_join_middle_bucket(selfjoin, &(list_temp->bucket), p->table, result))
        {
            fprintf(stderr, "execute_query filter_middle_bucket: Error\n");
            return -4;
        }
        list_temp=list_temp->next;
        p->node_count--;
    }
    pthread_mutex_lock(p->parts_mutex);
    (*(p->unfiltered_parts))--;
    if(*(p->unfiltered_parts)==0)
    {
        pthread_mutex_unlock(p->parts_mutex);
        //Merge the lists of all filters
        middle_list* self_join_result=create_middle_list();
        if(self_join_result==NULL)
        {
            fprintf(stderr, "run_filter_table_job: Error in create_result_list\n");
            destroy_filter_table_job(parameters);
            return -3;
        }
        merge_middle_list(p->lists, self_join_result);
        //delete old list and put the new one in its place
        delete_middle_list(p->exe_params->middle->tables[selfjoin->r.table_id].list);
        p->exe_params->middle->tables[selfjoin->r.table_id].list=self_join_result;
        //Keep only the execute_parameters (with the old job inside)
        job_query_parameters *exe_params=p->exe_params;
        exe_params->pred_index++;
        exe_params->this_job->run=run_execute_job;
        schedule_fast_job(exe_params->this_job->scheduler, exe_params->this_job);
        delete_list_array(p->lists);
        //        free(p->lists->lists);
        //        free(p->lists);
        //        p->lists=NULL;
        destroy_filter_table_job(parameters);
    }
    else
    {
        pthread_mutex_unlock(p->parts_mutex);
        destroy_filter_table_job(parameters);
    }
    return 0;
}
int run_join_job(void * parameters)
{
    job_join_parameters* p=(job_join_parameters*) parameters;
    //Do the join
    predicate_join *join=p->exe_params->query->predicates[p->exe_params->pred_index].p;
    int delete_r=1, delete_s=1;
    //Create middle lists in which we are going to store the join results
    middle_list *result_R=p->lists->lists[p->list_position][0];
    middle_list *result_S=p->lists->lists[p->list_position][1];
    if(result_R==NULL||result_S==NULL)
    {
        fprintf(stderr, "run_join_job: Error in given lists\n");
        destroy_join_job(parameters);
        return -3;
    }
    //B.3.4 Join
    if(p->exe_params->r==NULL&&p->exe_params->s==NULL)
    {
        return -4;
    }
    if(final_join_parallel(result_R, result_S, p->exe_params->r, p->exe_params->s, p->start_r, p->end_r, p->start_s))
    {
        fprintf(stderr, "run_join_job: Error in final_join\n");
        destroy_join_job(parameters);
        return -6;
    }
    //If last join_job merge the results, clean up, and return to the execute_job
    pthread_mutex_lock(p->parts_mutex);
    (*(p->unjoined_parts))--;
    if(*(p->unjoined_parts)==0)
    {
        pthread_mutex_unlock(p->parts_mutex);
        //Merge the lists of all joins
        result_R=create_middle_list();
        result_S=create_middle_list();
        if(result_R==NULL||result_S==NULL)
        {
            fprintf(stderr, "run_join_job: Error in create_result_list\n");
            destroy_join_job(parameters);
            return -3;
        }
        merge_middle_lists(p->lists, result_R, result_S);
        //B.3.5 Now go back to middleman
        //If the list exists then update it
        //If not then put the result list (result_R, result_S) in its place
        if(p->exe_params->middle->tables[join->r.table_id].list==NULL)
        {
            p->exe_params->middle->tables[join->r.table_id].list=result_R;
            delete_r=0;
        }
        else
        {
            middle_list *new_list=create_middle_list();
            if(result_R->number_of_nodes>0)
            {
                //Construct lookup table of the existing (old) list
#if defined(SERIAL_EXECUTION)||(defined(SERIAL_JOIN)&&defined(SERIAL_FILTER)&&defined(SERIAL_SELFJOIN))
                middle_list_bucket **lookup=construct_lookup_table(p->exe_params->middle->tables[join->r.table_id].list);
#else
                lookup_table *lookup=construct_lookup_table(p->exe_params->middle->tables[join->r.table_id].list);
#endif
                //Traverse result list and for every rowId find it in the old list and put
                //the result in the new_list
                middle_list_node *list_temp=result_R->head;
                while(list_temp!=NULL)
                {
                    if(update_middle_bucket(lookup, &(list_temp->bucket), new_list))
                    {
                        fprintf(stderr, "run_join_job: Error in update_middle_bucket\n");
                        destroy_join_job(parameters);
                        return -7;
                    }
                    list_temp=list_temp->next;
                }
#if defined(SERIAL_EXECUTION)||(defined(SERIAL_JOIN)&&defined(SERIAL_FILTER)&&defined(SERIAL_SELFJOIN))
                free(lookup);
#else
                delete_lookup_table(lookup);
#endif
            }
            delete_middle_list(p->exe_params->middle->tables[join->r.table_id].list);
            p->exe_params->middle->tables[join->r.table_id].list=new_list;
        }
        //Same procedure for S as above
        if(p->exe_params->middle->tables[join->s.table_id].list==NULL)
        {
            p->exe_params->middle->tables[join->s.table_id].list=result_S;
            delete_s=0;
        }
        else
        {
            middle_list *new_list=create_middle_list();
            if(result_S->number_of_nodes>0)
            {
#if defined(SERIAL_EXECUTION)||(defined(SERIAL_JOIN)&&defined(SERIAL_FILTER)&&defined(SERIAL_SELFJOIN))
                middle_list_bucket **lookup=construct_lookup_table(p->exe_params->middle->tables[join->s.table_id].list);
#else
                lookup_table *lookup=construct_lookup_table(p->exe_params->middle->tables[join->s.table_id].list);
#endif
                middle_list_node *list_temp=result_S->head;
                while(list_temp!=NULL)
                {
                    if(update_middle_bucket(lookup, &(list_temp->bucket), new_list))
                    {
                        fprintf(stderr, "run_join_job: Error in update_middle_bucket\n");
                        destroy_join_job(parameters);
                        return -8;
                    }
                    list_temp=list_temp->next;
                }
#if defined(SERIAL_EXECUTION)||(defined(SERIAL_JOIN)&&defined(SERIAL_FILTER)&&defined(SERIAL_SELFJOIN))
                free(lookup);
#else
                delete_lookup_table(lookup);
#endif
            }
            delete_middle_list(p->exe_params->middle->tables[join->s.table_id].list);
            p->exe_params->middle->tables[join->s.table_id].list=new_list;
        }
        //Keep only the execute_parameters (with the old job inside)
        job_query_parameters *exe_params=p->exe_params;
        //B.3.6 Free relations
        if(exe_params->r->num_tuples>0)
        {
            free(exe_params->r->tuples);
            exe_params->r->tuples=NULL;
        }
        free(exe_params->r);
        exe_params->r=NULL;
        if(exe_params->s->num_tuples>0)
        {
            free(exe_params->s->tuples);
            exe_params->s->tuples=NULL;
        }
        free(exe_params->s);
        exe_params->s=NULL;
        //TODO Add if for return !=0
        update_related_lists(exe_params->pred_index, exe_params->query, exe_params->joined_tables, exe_params->middle, delete_r, delete_s, result_R, result_S, NULL);
        exe_params->pred_index++;
        exe_params->this_job->run=run_execute_job;
        schedule_fast_job(exe_params->this_job->scheduler, exe_params->this_job);
        delete_list_array(p->lists);
        //        free(p->lists->lists);
        //        free(p->lists);
        //        p->lists=NULL;
        destroy_join_job(parameters);
    }
    else
    {
        pthread_mutex_unlock(p->parts_mutex);
        destroy_join_job(parameters);
    }
    return 0;
}
void destroy_join_job(void * parameters)
{
    job_join_parameters* p=(job_join_parameters*) parameters;
    if(p==NULL)
    {
        return;
    }
    free(p->this_job);
    free(p);
}
job* create_presort_job(job_query_parameters* p, relation** r, table_column* tc, pthread_mutex_t* mutex, bool sort, uint64_t* uns_rows)
{
    if(p==NULL||p->middle==NULL||(*r)!=NULL||p->query==NULL||p->tables==NULL||p->this_job==NULL||
       tc==NULL||mutex==NULL||uns_rows==NULL)
    {
        fprintf(stderr, "create_presort_job: NULL parameter\n");
        return NULL;
    }
    job* newjob=malloc(sizeof(job));
    if(newjob==NULL)
    {
        perror("create_presort_job: malloc error");
        return NULL;
    }
    job_presort_parameters* par=malloc(sizeof(job_presort_parameters));
    if(par==NULL)
    {
        perror("create_presort_job: malloc error");
        free(newjob);
        return NULL;
    }
    par->r=r;
    par->r_s=NULL;
    par->join=tc;
    par->q_params=p;
    par->mutex=mutex;
    par->sort=sort;
    par->unsorted_rows=uns_rows;
    par->this_job=newjob;
    newjob->scheduler=p->this_job->scheduler;
    newjob->parameters=(void*) par;
    newjob->run=&run_presort_job;
    newjob->destroy=&destroy_presort_job;
    return newjob;
}
job* create_sort_job(job_query_parameters* p, relation* r, relation* r_s, pthread_mutex_t* mutex, uint64_t* uns_rows, unsigned short byte, uint64_t start, uint64_t end)
{
    if(p==NULL||r==NULL||r_s==NULL||mutex==NULL||uns_rows==NULL||byte>9||start>end)
    {
        fprintf(stderr, "create_sort_job: wrong parameter\n");
        return NULL;
    }
    job* newjob=malloc(sizeof(job));
    if(newjob==NULL)
    {
        perror("create_sort_job: malloc error");
        return NULL;
    }
    job_sort_parameters* par=malloc(sizeof(job_sort_parameters));
    if(par==NULL)
    {
        perror("create_sort_job: malloc error");
        free(newjob);
        return NULL;
    }
    par->r=r;
    par->r_s=r_s;
    par->mutex=mutex;
    par->unsorted_rows=uns_rows;
    par->q_params=p;
    par->win.byte=byte;
    par->win.start=start;
    par->win.end=end;
    par->this_job=newjob;
    newjob->scheduler=p->this_job->scheduler;
    newjob->parameters=(void*) par;
    newjob->run=&run_sort_job;
    newjob->destroy=&destroy_sort_job;
    return newjob;
}
job* create_projection_job(job_query_parameters* p, pthread_mutex_t* mutex, uint64_t* projections_left, projection* pr, uint32_t pr_index)
{
    if(p==NULL||mutex==NULL||projections_left==NULL||pr==NULL)
    {
        fprintf(stderr, "create_projection_job: wrong parameter\n");
        return NULL;
    }
    job* newjob=malloc(sizeof(job));
    if(newjob==NULL)
    {
        perror("create_projection_job: malloc error");
        return NULL;
    }
    job_projection_parameters* par=malloc(sizeof(job_projection_parameters));
    if(par==NULL)
    {
        perror("create_projection_job: malloc error");
        free(newjob);
        return NULL;
    }
    par->mutex=mutex;
    par->query_parameters=p;
    par->projections_left=projections_left;
    par->this_job=newjob;
    par->projection=pr;
    par->projection_index=pr_index;
    newjob->scheduler=p->this_job->scheduler;
    newjob->parameters=(void*) par;
    newjob->run=&run_projection_job;
    newjob->destroy=&destroy_projection_job;
    return newjob;
}
int run_presort_job(void* parameters)
{
    job_presort_parameters* p=(job_presort_parameters*) parameters;
    if(p->mutex==NULL||(*p->r)!=NULL||p->r_s!=NULL||p->unsorted_rows==NULL||*(p->unsorted_rows)!=1||
       p->join==NULL||p->q_params==NULL)
    {
        fprintf(stderr, "run_presort_job: wrong parameters\n");
        destroy_presort_job(parameters);
        return -1;
    }
    //Find original table
    table *table=get_table(p->q_params->tables, p->q_params->query->table_ids[p->join->table_id]);
    if(table==NULL)
    {
        fprintf(stderr, "run_presort_job:  Null get_table\n");
        destroy_presort_job(parameters);
        return -3;
    }
    //Construct relation
    //Check if table exists in middleman
    //If yes use it else, if not take if from the original table
    if(p->q_params->middle->tables[p->join->table_id].list==NULL)
    {
        (*p->r)=construct_relation_from_table(table, p->join->column_id);
    }
    else
    {
        (*p->r)=malloc(sizeof(relation));
        if((*p->r)==NULL)
        {
            fprintf(stderr, "run_presort_job: Cannot allocate memory\n");
            destroy_presort_job(parameters);
            return -4;
        }
        (*p->r)->num_tuples=middle_list_get_number_of_records(p->q_params->middle->tables[p->join->table_id].list);
        if((*p->r)->num_tuples>0)
        {
            (*p->r)->tuples=malloc(((*p->r)->num_tuples)*sizeof(tuple));
            if((*p->r)->tuples==NULL)
            {
                fprintf(stderr, "run_presort_job: Cannot allocate memory\n");
                destroy_presort_job(parameters);
                return -4;
            }
            //Construct relation from middleman
            uint64_t counter=0;
            middle_list_node *list_temp=p->q_params->middle->tables[p->join->table_id].list->head;
            while(list_temp!=NULL)
            {
                construct_relation_from_middleman(&(list_temp->bucket), table, (*p->r), p->join->column_id, &counter);
                list_temp=list_temp->next;
            }
        }
    }
    //Relation is ready... Sort if necessary
    if((*p->r)->num_tuples==0||!p->sort)
    {
        pthread_mutex_lock(p->mutex);
        *(p->unsorted_rows)=0;
        pthread_mutex_unlock(p->mutex);
        bool create_prejoin=false;
        if(p->mutex==&p->q_params->r_mutex)
        {
            pthread_mutex_lock(&p->q_params->s_mutex);
            if(p->q_params->s_counter==0)
            {
                create_prejoin=true;
            }
            pthread_mutex_unlock(&p->q_params->s_mutex);
        }
        else if(p->mutex==&p->q_params->s_mutex)
        {
            pthread_mutex_lock(&p->q_params->r_mutex);
            if(p->q_params->r_counter==0)
            {
                create_prejoin=true;
            }
            pthread_mutex_unlock(&p->q_params->r_mutex);
        }
        else
        {
            fprintf(stderr, "run_presort_job:  mutex error\n");
        }
        if(create_prejoin)
        {
            pthread_mutex_lock(&p->q_params->q_mutex);
            if(p->q_params->is_prejoin_scheduled)
            {
                create_prejoin=false;
            }
            else
            {
                p->q_params->is_prejoin_scheduled=true;
            }
            pthread_mutex_unlock(&p->q_params->q_mutex);
            if(create_prejoin)
            {
                p->q_params->this_job->run=run_prejoin_job;
                schedule_fast_job(p->q_params->this_job->scheduler, p->q_params->this_job);
            }
        }
        destroy_presort_job(parameters);
        return 0;
    }
    else if(p->sort)
    {
#if defined(SERIAL_SORTING)
        printf("SERIAL SORTING\n");
        if(radix_sort((*p->r)))
        {
            fprintf(stderr, "execute_query: Error in radix_sort\n");
            return -5;
        }
        pthread_mutex_lock(p->mutex);
        *(p->unsorted_rows)=0;
        pthread_mutex_unlock(p->mutex);
        bool create_prejoin=false;
        if(p->mutex==&p->q_params->r_mutex)
        {
            pthread_mutex_lock(&p->q_params->s_mutex);
            if(p->q_params->s_counter==0)
            {
                create_prejoin=true;
            }
            pthread_mutex_unlock(&p->q_params->s_mutex);
        }
        else if(p->mutex==&p->q_params->s_mutex)
        {
            pthread_mutex_lock(&p->q_params->r_mutex);
            if(p->q_params->r_counter==0)
            {
                create_prejoin=true;
            }
            pthread_mutex_unlock(&p->q_params->r_mutex);
        }
        else
        {
            fprintf(stderr, "run_presort_job:  mutex error\n");
        }
        if(create_prejoin)
        {
            pthread_mutex_lock(&p->q_params->q_mutex);
            if(p->q_params->is_prejoin_scheduled)
            {
                create_prejoin=false;
            }
            else
            {
                p->q_params->is_prejoin_scheduled=true;
            }
            pthread_mutex_unlock(&p->q_params->q_mutex);
            if(create_prejoin)
            {
                p->q_params->this_job->run=run_prejoin_job;
                schedule_fast_job(p->q_params->this_job->scheduler, p->q_params->this_job);
            }
        }
        destroy_presort_job(parameters);
        return 0;
#else
        //Create array to help with the sorting
        p->r_s=malloc(sizeof(relation));
        if(p->r_s==NULL)
        {
            perror("run_presort_job: malloc error");
            destroy_presort_job(parameters);
            return -1;
        }
        p->r_s->num_tuples=(*p->r)->num_tuples;
        p->r_s->tuples=malloc(p->r_s->num_tuples*sizeof(tuple));
        if(p->r_s->tuples==NULL)
        {
            perror("run_presort_job: malloc error");
            destroy_presort_job(parameters);
            return -1;
        }
        job* newjob=create_sort_job(p->q_params, *p->r, p->r_s, p->mutex, p->unsorted_rows, 1, 0, (*p->r)->num_tuples);
        if(newjob==NULL)
        {
            destroy_presort_job(parameters);
            return -1;
        }
        //TODO Add if
        pthread_mutex_lock(p->mutex);
        *(p->unsorted_rows)=(*p->r)->num_tuples;
        pthread_mutex_unlock(p->mutex);
        schedule_fast_job(p->this_job->scheduler, newjob);
        destroy_presort_job(parameters);
        return 0;
#endif
    }
    destroy_presort_job(parameters);
    return 0;
}
int run_sort_job(void* parameters)
{
    job_sort_parameters* p=(job_sort_parameters*) parameters;
    if(p==NULL||p->mutex==NULL||p->r==NULL||p->r->num_tuples==0||p->r->tuples==NULL||
       p->r_s==NULL||p->r_s->num_tuples==0||p->r_s->tuples==NULL||p->unsorted_rows==NULL||
       p->this_job==NULL||p->win.byte>9||p->win.start>=p->win.end)
    {
        fprintf(stderr, "run_sort_job: wrong parameters\n");
        destroy_sort_job(parameters);
        return -1;
    }
    //Check if less than 64KB
    if((p->win.end-p->win.start)*sizeof(tuple)<64*1024||p->win.byte==9)
    {
        //Choose whether to place result in array or auxiliary array
        if(p->win.byte%2==0)
        {
            quicksort(p->r->tuples, p->win.start, p->win.end-1);
            copy_relation(p->r, p->r_s, p->win.start, p->win.end);
        }
        else
        {
            quicksort(p->r->tuples, p->win.start, p->win.end-1);
        }
        pthread_mutex_lock(p->mutex);
        if(*(p->unsorted_rows)<(p->win.end-p->win.start))
        {
            printf("\t\t\tError Rows Before %"PRIu64"\n", *(p->unsorted_rows));
        }
        *(p->unsorted_rows)-=(p->win.end-p->win.start);
        if(*(p->unsorted_rows)==0)
        {//Finished sorting delete the r_s*
            pthread_mutex_unlock(p->mutex);
            if(p->win.byte%2==0)
            {
                relation *temp=p->r;
                p->r=p->r_s;
                p->r_s=temp;
            }
            if(p->r_s->tuples!=NULL)
            {
                free(p->r_s->tuples);
                p->r_s->tuples=NULL;
            }
            if(p->r_s!=NULL)
            {
                free(p->r_s);
                p->r_s=NULL;
            }
            bool create_prejoin=false;
            if(p->mutex==&p->q_params->r_mutex)
            {
                pthread_mutex_lock(&p->q_params->s_mutex);
                if(p->q_params->s_counter==0)
                {
                    create_prejoin=true;
                }
                pthread_mutex_unlock(&p->q_params->s_mutex);
            }
            else if(p->mutex==&p->q_params->s_mutex)
            {
                pthread_mutex_lock(&p->q_params->r_mutex);
                if(p->q_params->r_counter==0)
                {
                    create_prejoin=true;
                }
                pthread_mutex_unlock(&p->q_params->r_mutex);
            }
            else
            {
                fprintf(stderr, "run_presort_job:  mutex error\n");
            }
            if(create_prejoin)
            {
                pthread_mutex_lock(&p->q_params->q_mutex);
                if(p->q_params->is_prejoin_scheduled)
                {
                    create_prejoin=false;
                }
                else
                {
                    p->q_params->is_prejoin_scheduled=true;
                }
                pthread_mutex_unlock(&p->q_params->q_mutex);
                if(create_prejoin)
                {
                    p->q_params->this_job->run=run_prejoin_job;
                    schedule_fast_job(p->q_params->this_job->scheduler, p->q_params->this_job);
                }
            }
        }
        else
        {
            pthread_mutex_unlock(p->mutex);
        }
        
        destroy_sort_job(parameters);
        return 0;
    }
    else
    {
        uint64_t *hist=malloc(HIST_SIZE*sizeof(uint64_t));
        if(hist==NULL)
        {
            perror("radix_sort: malloc error");
            destroy_sort_job(parameters);
            return -1;
        }
        for(uint64_t i=0; i<HIST_SIZE; i++)
        {
            hist[i]=0;
        }
        int res=create_histogram(p->r, p->win.start, p->win.end, hist, p->win.byte);
        if(res)
        {
            destroy_sort_job(parameters);
            return -3;
        }
        res=transform_to_psum(hist);
        if(res)
        {
            destroy_sort_job(parameters);
            return -4;
        }
        copy_relation_with_psum(p->r, p->r_s, p->win.start, p->win.end, hist, p->win.byte);
        uint64_t start=p->win.start;
        for(uint64_t i=0; i<HIST_SIZE; i++)
        {
            if(start<p->win.start+hist[i])
            {
                job* newjob=create_sort_job(p->q_params, p->r_s, p->r, p->mutex, p->unsorted_rows, p->win.byte+1, start, p->win.start+hist[i]);
                if(newjob==NULL)
                {
                    destroy_sort_job(parameters);
                    return -1;
                }
                //TODO Add if
                schedule_fast_job(p->this_job->scheduler, newjob);
            }
            if(hist[i]+p->win.start>p->win.end)
            {
                break;
            }
            start=p->win.start+hist[i];
        }
        free(hist);
    }
    //    pthread_mutex_unlock(p->mutex);
    destroy_sort_job(parameters);
    return 0;
}
int run_projection_job(void* parameters)
{
    //TODO ADD frees/destroy etc
    job_projection_parameters* p=(job_projection_parameters*) parameters;
    if(p==NULL||p->mutex==NULL||p->projections_left==NULL||p->this_job==NULL)
    {
        fprintf(stderr, "run_projection_job: wrong parameters\n");
        return -1;
    }
    if(p->query_parameters->middle->tables[p->projection->column_to_project.table_id].list==NULL||p->query_parameters->middle->tables[p->projection->column_to_project.table_id].list->number_of_nodes==0)
    {
#if defined(SORTED_PROJECTIONS)
            if(store_projection_in_scheduler(p->this_job->scheduler, p->query_parameters->query_id, p->query_parameters->query->number_of_projections, p->projection_index, 0)!=0)
            {
                return -4;
            }
#else
            fprintf(stderr, "\e[1;33mNULL \e[0m");
#endif
    }
    else
    {
        table *original_table=get_table(p->query_parameters->tables, p->query_parameters->query->table_ids[p->projection->column_to_project.table_id]);
        if(original_table==NULL)
        {
            fprintf(stderr, "calculate_projections: Table not found\n");
            return -5;
        }
        uint64_t sum=0;
        middle_list_node *list_temp=p->query_parameters->middle->tables[p->projection->column_to_project.table_id].list->head;
        while(list_temp!=NULL)
        {
            calculate_sum(p->projection, &(list_temp->bucket), original_table, &sum);
            list_temp=list_temp->next;
        }
#if defined(SORTED_PROJECTIONS)
        if(store_projection_in_scheduler(p->this_job->scheduler, p->query_parameters->query_id, p->query_parameters->query->number_of_projections, p->projection_index, sum)!=0)
        {
            return -4;
        }
#else
        fprintf(stderr, "\e[1;33m%" PRIu64 " \e[0m", sum);
#endif
    }
    pthread_mutex_lock(p->mutex);
    *(p->projections_left)-=1;
    if(*(p->projections_left)==0)
    {//All projections finished
        pthread_mutex_unlock(p->mutex);
#if !defined(SORTED_PROJECTIONS)
        fprintf(stderr, "\n");
#endif
        destroy_query_job((void*) p->query_parameters);
    }
    else
    {
        pthread_mutex_unlock(p->mutex);
    }
    destroy_projection_job(parameters);
    return 0;
}
