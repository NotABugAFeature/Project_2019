#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "job_scheduler.h"
#include "sort_merge_join.h"
#include "execute_query.h"
job_scheduler* create_job_scheduler()
{
    job_scheduler* js=malloc(sizeof(job_scheduler));
    if(js==NULL)
    {
        perror("create_job_scheduler: malloc error");
        return NULL;
    }
    if(sem_init(&js->fifo_job_counter_sem, 0, 0)!=0)
    {
        perror("create_job_scheduler fifo_job_counter_sem initialization");
        free(js);
        return NULL;
    }
    if(pthread_mutex_init(&js->job_fifo_mutex, NULL)!=0)
    {
        perror("create_job_scheduler fifo_job_mutex initialization");
        sem_destroy(&js->fifo_job_counter_sem);
        free(js);
        return NULL;
    }
    js->fifo=create_job_fifo();
    if(js->fifo==NULL)
    {
        pthread_mutex_destroy(&js->job_fifo_mutex);
        sem_destroy(&js->fifo_job_counter_sem);
        free(js);
        return NULL;
    }
    js->job_count=0;
    return js;
}
void delete_job_scheduler(job_scheduler* js)
{
    if(js==NULL)
    {
        return;
    }
    if(js->fifo!=NULL)
    {
        delete_job_fifo(js->fifo);
        js->fifo=NULL;
    }
    pthread_mutex_destroy(&js->job_fifo_mutex);
    sem_destroy(&js->fifo_job_counter_sem);
    free(js);
    js=NULL;
}
int schedule_job(job_scheduler* js, job* j)
{
    if(js==NULL||js->fifo==NULL||j==NULL)
    {
        fprintf(stderr, "schedule_job: NULL parameter\n");
        return -1;
    }
    pthread_mutex_lock(&js->job_fifo_mutex);
    //Append to fifo
    //TODO Add checks
    if(append_to_job_fifo(js->fifo, j)==0)
    {
        js->job_count++;
        pthread_mutex_unlock(&js->job_fifo_mutex);
        sem_post(&js->fifo_job_counter_sem);
        return 0;
    }
    pthread_mutex_unlock(&js->job_fifo_mutex);
    return -2;
}
job* get_job(job_scheduler* js)
{
    if(js==NULL||js->fifo==NULL)
    {
        fprintf(stderr, "get_job: NULL parameter\n");
        return NULL;
    }
    sem_wait(&js->fifo_job_counter_sem);
    pthread_mutex_lock(&js->job_fifo_mutex);
    //Pop from fifo
    //TODO Add checks
    job* j=pop_from_job_fifo(js->fifo);
    pthread_mutex_unlock(&js->job_fifo_mutex);
    return j;
}
job* create_query_job(job_scheduler* jb, char* query_str, table_index* ti, uint64_t id)
{
    if(jb==NULL||query_str==NULL||ti==NULL)
    {
        fprintf(stderr, "create_query_job: NULL parameter\n");
        return NULL;
    }
    job* newjob=malloc(sizeof(job));
    if(newjob==NULL)
    {
        perror("create_query_job: malloc error");
        return NULL;
    }
    job_query_parameters* par=malloc(sizeof(job_query_parameters));
    if(par==NULL)
    {
        perror("create_query_job: malloc error");
        free(newjob);
        return NULL;
    }
    par->query_str=query_str;
    par->tables=ti;
    par->query_id=id;
    par->middle=NULL;
    par->bool_array=NULL;
    par->query=NULL;
    par->b_counter=0;
    par->joined_tables=NULL;
    par->pred_index=0;
    par->this_job=newjob;
    par->r=NULL;
    par->s=NULL;
    //TODO check mutex init
    pthread_mutex_init(&par->r_mutex, NULL);
    pthread_mutex_init(&par->s_mutex, NULL);
    par->unsorted_r_rows=0;
    par->unsorted_s_rows=0;
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
    p->this_job->run=&run_execute_job;
    if(p->this_job->run(parameters)<0)
    {
        return -4;
    }
    //TODO Add deletes if needed
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
    //    if(execute_query_parallel(p->query, p->tables, p->bool_array,&p->b_counter,&p->pred_index,&p->middle,&p->joined_tables)!=0)
    int result=execute_query_parallel(p);
    //TODO Fix errors
    if(result<0)
    {
        printf("\n\nERRRRRRRRRRRRRRRRRRRRRRROOOOOOOOOOOOOOOORRRRRRRRRR\n\n");
        destroy_query_job(parameters);
        return -1;
    }
    else if(result==0)
    {
        calculate_projections(p->query, p->tables, p->middle);
        destroy_query_job(parameters);
    }
    else if(result==1)
    {

    }
    else
    {
        printf("\n\nERRRRRRRRRRRRRRRRRRRRRRROOOOOOOOOOOOOOOORRRRRRRRRR\n\n");
        destroy_query_job(parameters);
    }
    return 0;
}
void destroy_query_job(void* parameters)
{//TODO Check frees
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
        free(p->query);
        p->query=NULL;
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
    free(p);
    p=NULL;
}
void destroy_presort_job(void* parameters)
{//TODO Check frees
}
void destroy_sort_job(void* parameters)
{//TODO Check frees
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

    par->start_r = start_r;
    par->end_r = end_r;
    par->start_s = start_s;
    par->unjoined_parts = unjoined_parts;
    par->parts_mutex = parts_mutex;
    par->list_position = list_position;
    par->lists = lists;
    par->exe_params = exe_params;
    par->this_job = newjob;
    newjob->scheduler = exe_params->this_job->scheduler;
    newjob->parameters = (void *) par;
    newjob->run = &run_join_job;
    newjob->destroy = &destroy_join_job;
    return newjob;
}

int run_join_job(void * parameters)
{
	job_join_parameters* p=(job_join_parameters*) parameters;

	//Do the join
    predicate_join *join=p->exe_params->query->predicates[p->exe_params->pred_index].p;
    int delete_r=1, delete_s=1;
    //Create middle lists in which we are going to store the join results
    middle_list *result_R = par->lists[par->list_position][0];
    middle_list *result_S = par->lists[par->list_position][1];
    if(result_R==NULL || result_S==NULL)
    {
        fprintf(stderr, "run_join_job: Error in given lists\n");
        return -3;
    }
    //B.3.4 Join
    if(p->exe_params->r==NULL&&p->exe_params->s==NULL)
    {

        return -4;
    }
    if(final_join(result_R, result_S, p->exe_params->r, p->exe_params->s, p->start_r, p->end_r, p->start_s))
    {
        fprintf(stderr, "run_join_job: Error in final_join\n");
        return -6;
    }
    

    //If last join_job merge the results, clean up, and return to the execute_job
    pthread_mutex_lock(p->parts_mutex);
    p->unjoined_parts--;
    if(p->unjoined_parts == 0)
    {
    	pthread_mutex_unlock(p->parts_mutex);

    	//Merge the lists of all joins
    	result_R = create_result_list();
    	result_S = create_result_list();
    	if(result_R == NULL || result_S == NULL)
    	{
    		fprintf(stderr, "run_join_job: Error in create_result_list\n");
    		return -3
    	}
    	merge_middle_lists(par->lists, result_R, result_S);

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
    	        middle_list_bucket **lookup=construct_lookup_table(p->exe_params->middle->tables[join->r.table_id].list);
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
    	        free(lookup);
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
    	        middle_list_bucket **lookup=construct_lookup_table(p->exe_params->middle->tables[join->s.table_id].list);
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
    	        free(lookup);
    	    }
    	    delete_middle_list(p->exe_params->middle->tables[join->s.table_id].list);
    	    p->exe_params->middle->tables[join->s.table_id].list=new_list;
    	}

    	//Keep only the execute_parameters (with the old job inside)
    	job_query_parameters *exe_params = p->exe_params;
    	free(p->unjoined_parts);
    	pthread_mutex_destroy(p->parts_mutex);
    	free(p->parts_mutex);
    	free(p);

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
    	schedule_job(exe_params->this_job->scheduler, exe_params->this_job);
    }
    else {
    	pthread_mutex_unlock(p->parts_mutex);
    }

    
    return 0;
}

void destroy_join_job(void * parameters)
{
	//TODO
}

int run_prejoin_job(void * parameters)
{
	job_query_parameters* p=(job_query_parameters*) parameters;
	if(p==NULL||p->query_str!=NULL||p->bool_array==NULL||p->query==NULL||p->tables==NULL||p->this_job==NULL||
	   p->b_counter==0||p->joined_tables==NULL||p->middle==NULL)
	{
	    fprintf(stderr, "run_prejoin_job: wrong parameters\n");
	    destroy_query_job(parameters);
	    return -1;
	}

	//Check if the sorting is complete
    pthread_mutex_lock(&p->r_mutex);
//    printf("Unsorted R:%"PRIu64"\n",p->unsorted_r_rows);
    if(p->unsorted_r_rows!=0)
    {
        pthread_mutex_unlock(&p->r_mutex);
        //Append to fifo
        //TODO Add checks
        schedule_job(p->this_job->scheduler, p->this_job);
        return 0;
    }
    pthread_mutex_unlock(&p->r_mutex);
    pthread_mutex_lock(&p->s_mutex);
//    printf("Unsorted S:%"PRIu64"\n",p->unsorted_s_rows);
    if(p->unsorted_s_rows!=0)
    {
        pthread_mutex_unlock(&p->s_mutex);
        schedule_job(p->this_job->scheduler, p->this_job);
        return 0;
    }
    pthread_mutex_unlock(&p->s_mutex);


    //printf("\e[1;31mR->num_tuples = %" PRIu64 "\e[0m\n", p->r->num_tuples);

    //Create mutex for unjoined_parts
	pthread_mutex_t *parts_mutex = malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(parts_mutex, NULL);
    uint64_t *unjoined_parts = malloc(sizeof(uint64_t));

    //Figure out size of parts
	uint64_t parts = 3;
	uint64_t small_size = p->r->num_tuples/parts;
	uint64_t extra = p->r->num_tuples%parts;
	*unjoined_parts = parts;

	//Create a list_array to keep the results of each part
	list_array *la = create_list_array(parts);

	//printf("\e[1;36mR tuples: %" PRIu64 ", S tuples: %" PRIu64 "\e[0m\n", p->r->num_tuples, p->s->num_tuples);
	uint64_t start_r, end_r = 0, start_s = 0;
	for(uint64_t i=0; i<parts; i++)
	{
		start_r = end_r;
		end_r = start_r + small_size;
		if(i < extra)
		{
			end_r++;
		}
		//printf("\e[1;36mRange R: %" PRIu64 " - %" PRIu64 " starting at %" PRIu64 "\e[0m\n", start_r, end_r, p->r->tuples[start_r].key);
		while(p->s->tuples[start_s].key < p->r->tuples[start_r].key && start_s < p->s->num_tuples)
		{
			//printf("\e[1;36mS[%" PRIu64 "].key = %" PRIu64 ", and R[%" PRIu64 "] = %" PRIu64 "\e[0m\n", start_s, p->s->tuples[start_s].key, start_r, p->r->tuples[start_r].key);
			start_s++;
		}

		//printf("\e[1;36mRange S: %" PRIu64 " starting at %" PRIu64 "\e[0m\n", start_s, p->s->tuples[start_s].key);
		job *newjob = create_join_job(start_r, end_r, start_s, unjoined_parts, parts_mutex, la, i, p);
	}

    return 0;
}

void destroy_prejoin_job(void * parameters)
{
	//TODO
}


/*
int run_join_job(void* parameters)
{
    job_query_parameters* p=(job_query_parameters*) parameters;
    if(p==NULL||p->query_str!=NULL||p->bool_array==NULL||p->query==NULL||p->tables==NULL||p->this_job==NULL||
       p->b_counter==0||p->joined_tables==NULL||p->middle==NULL)
    {
        fprintf(stderr, "run_join_job: wrong parameters\n");
        destroy_query_job(parameters);
        return -1;
    }
    //TODO Add checks
    //Check if the sorting is complete
    pthread_mutex_lock(&p->r_mutex);
//    printf("Unsorted R:%"PRIu64"\n",p->unsorted_r_rows);
    if(p->unsorted_r_rows!=0)
    {
        pthread_mutex_unlock(&p->r_mutex);
        //Append to fifo
        //TODO Add checks
        schedule_job(p->this_job->scheduler, p->this_job);
        return 0;
    }
    pthread_mutex_unlock(&p->r_mutex);
    pthread_mutex_lock(&p->s_mutex);
//    printf("Unsorted S:%"PRIu64"\n",p->unsorted_s_rows);
    if(p->unsorted_s_rows!=0)
    {
        pthread_mutex_unlock(&p->s_mutex);
        schedule_job(p->this_job->scheduler, p->this_job);
        return 0;
    }
    pthread_mutex_unlock(&p->s_mutex);
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
            middle_list_bucket **lookup=construct_lookup_table(p->middle->tables[join->r.table_id].list);
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
            free(lookup);
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
            middle_list_bucket **lookup=construct_lookup_table(p->middle->tables[join->s.table_id].list);
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
            free(lookup);
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
    schedule_job(p->this_job->scheduler, p->this_job);
    return 0;
}*/
job* create_presort_job(job_query_parameters* p, relation** r, table_column* tc, pthread_mutex_t* mutex, bool sort, uint64_t* uns_rows)
{
    if(p->middle==NULL||(*r)!=NULL||p->query==NULL||p->tables==NULL||p->this_job==NULL||
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
    par->middle=p->middle;
    par->mutex=mutex;
    par->query=p->query;
    par->sort=sort;
    par->tables=p->tables;
    par->unsorted_r_rows=uns_rows;
    par->this_job=newjob;
    newjob->scheduler=p->this_job->scheduler;
    newjob->parameters=(void*) par;
    newjob->run=&run_presort_job;
    newjob->destroy=&destroy_presort_job;
    return newjob;
}
job* create_sort_job(job_scheduler* js,relation* r, relation* r_s, pthread_mutex_t* mutex, uint64_t* uns_rows,unsigned short byte,uint64_t start,uint64_t end)
{
    if(js==NULL||r==NULL||r_s==NULL||mutex==NULL||uns_rows==NULL||byte>9||start>end)
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
    par->unsorted_r_rows=uns_rows;
    par->win.byte=byte;
    par->win.start=start;
    par->win.end=end;
    par->this_job=newjob;
    newjob->scheduler=js;
    newjob->parameters=(void*) par;
    newjob->run=&run_sort_job;
    newjob->destroy=&destroy_sort_job;
    return newjob;
}
int run_presort_job(void* parameters)
{
    job_presort_parameters* p=(job_presort_parameters*) parameters;
    if(p->mutex==NULL||(*p->r)!=NULL||p->r_s!=NULL||p->unsorted_r_rows==NULL||*(p->unsorted_r_rows)!=1||
       p->join==NULL||p->middle==NULL||p->query==NULL||p->tables==NULL)
    {
        fprintf(stderr, "run_presort_job: wrong parameters\n");
        destroy_presort_job(parameters);
        return -1;
    }
    //Find original table
    table *table=get_table(p->tables, p->query->table_ids[p->join->table_id]);
    if(table==NULL)
    {
        fprintf(stderr, "run_presort_job:  Null get_table\n");
        destroy_presort_job(parameters);
        return -3;
    }
    //Construct relation
    //Check if table exists in middleman
    //If yes use it else, if not take if from the original table
    if(p->middle->tables[p->join->table_id].list==NULL)
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
        (*p->r)->num_tuples=middle_list_get_number_of_records(p->middle->tables[p->join->table_id].list);
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
            middle_list_node *list_temp=p->middle->tables[p->join->table_id].list->head;
            while(list_temp!=NULL)
            {
                construct_relation_from_middleman(&(list_temp->bucket), table, (*p->r), p->join->column_id, &counter);
                list_temp=list_temp->next;
            }
        }
    }
    //Relation is ready... Sort if necessary
    if(p->sort)
    {
        if((*p->r)->num_tuples==0)
        {
            pthread_mutex_lock(p->mutex);
            *(p->unsorted_r_rows)=0;
            pthread_mutex_unlock(p->mutex);
            destroy_presort_job(parameters);
            return 0;
        }
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
        job* newjob=create_sort_job(p->this_job->scheduler,*p->r,p->r_s,p->mutex,p->unsorted_r_rows,1,0,(*p->r)->num_tuples);
        if(newjob==NULL)
        {
            destroy_presort_job(parameters);
            return -1;
        }
        //TODO Add if
        pthread_mutex_lock(p->mutex);
        *(p->unsorted_r_rows)=(*p->r)->num_tuples;
        pthread_mutex_unlock(p->mutex);
        schedule_job(p->this_job->scheduler,newjob);
        destroy_presort_job(parameters);
        return 0;
//        if(radix_sort((*p->r)))
//        {
//            fprintf(stderr, "execute_query: Error in radix_sort\n");
//            return -5;
//        }
    }
    pthread_mutex_lock(p->mutex);
    *(p->unsorted_r_rows)=0;
    pthread_mutex_unlock(p->mutex);
    destroy_presort_job(parameters);
    return 0;
}
int run_sort_job(void* parameters)
{//TODO ADD frees/destroy etc
//    printf("SortJob\n");
//    pthread_mutex_lock(p->mutex);
    job_sort_parameters* p=(job_sort_parameters*) parameters;
    if(p==NULL||p->mutex==NULL||p->r==NULL||p->r->num_tuples==0||p->r->tuples==NULL||
       p->r_s==NULL||p->r_s->num_tuples==0||p->r_s->tuples==NULL||p->unsorted_r_rows==NULL||
       p->this_job==NULL||p->win.byte>9||p->win.start>=p->win.end)
    {
        fprintf(stderr, "run_sort_job: wrong parameters\n");
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
        if(*(p->unsorted_r_rows)<(p->win.end-p->win.start))
        {
            printf("\t\t\tError Rows Before %"PRIu64"\n",*(p->unsorted_r_rows));
        }
        *(p->unsorted_r_rows)-=(p->win.end-p->win.start);
        if(*(p->unsorted_r_rows)==0)
        {//Finished sorting delete the r_s*
            if(p->win.byte%2==0)
            {
                relation *temp=p->r;
                p->r=p->r_s;
                p->r_s=temp;
            }
            free(p->r_s);
            p->r_s=NULL;
        }
        pthread_mutex_unlock(p->mutex);
        return 0;
    }
    else
    {
        uint64_t *hist=malloc(HIST_SIZE*sizeof(uint64_t));
        if(hist==NULL)
        {
            perror("radix_sort: malloc error");
            return -1;
        }
        for(uint64_t i=0; i<HIST_SIZE; i++)
        {
            hist[i]=0;
        }
        int res=create_histogram(p->r, p->win.start, p->win.end, hist, p->win.byte);
        if(res)
        {
            return -3;
        }
        res=transform_to_psum(hist);
        if(res)
        {
            return -4;
        }
        copy_relation_with_psum(p->r, p->r_s, p->win.start, p->win.end, hist, p->win.byte);
        //Recursively sort every part of the array
        uint64_t start=p->win.start;
        for(uint64_t i=0; i<HIST_SIZE; i++)
        {
            if(start<p->win.start+hist[i])
            {
                job* newjob=create_sort_job(p->this_job->scheduler,p->r_s,p->r,p->mutex,p->unsorted_r_rows,p->win.byte+1,start,p->win.start+hist[i]);
                if(newjob==NULL)
                {
                    destroy_sort_job(parameters);
                    return -1;
                }
                //TODO Add if
                schedule_job(p->this_job->scheduler,newjob);
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
    return 0;
}
