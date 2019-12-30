#include <stdio.h>
#include <stdlib.h>
#include "job_scheduler.h"
#include "query.h"
job_scheduler* create_job_scheduler()
{
    job_scheduler* js=malloc(sizeof(job_scheduler));
    if(js==NULL)
    {
        perror("create_job_scheduler: malloc error");
        return NULL;
    }
    js->fifo=create_job_fifo();
    if(js->fifo==NULL)
    {
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
    if(append_to_job_fifo(js->fifo,j)==0)
    {
        js->job_count++;
        return 0;
    }
    return -2;
}
job* get_job(job_scheduler* js)
{
    if(js==NULL||js->fifo==NULL)
    {
        fprintf(stderr, "get_job: NULL parameter\n");
        return NULL;
    }
    return pop_from_job_fifo(js->fifo);
}
job* create_query_job(char* query_str, table_index* ti,uint64_t id)
{
    if(query_str==NULL||ti==NULL)
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
    newjob->parameters=(void*) par;
    newjob->run=&run_query_job;
    newjob->print=&print_query_job;
    newjob->destroy=&destroy_query_job;
    return newjob;
}
int run_query_job(void* parameters)
{
    job_query_parameters* p=(job_query_parameters*) parameters;
    if(p==NULL||p->query_str==NULL)
    {
        fprintf(stderr, "NULL parameters\n");
        return -1;
    }
    if(p->tables==NULL)
    {
        free(p->query_str);
        p->query_str=NULL;
        fprintf(stderr, "NULL query str\n");
        return -2;
    }
    //    printf("The query to analyze: %s\n",p->query_str);
    query* q=create_query();
    if(q==NULL)
    {
        free(p->query_str);
        p->query_str=NULL;
        return -3;
    }
    if(analyze_query(p->query_str, q)!=0)
    {
        delete_query(q);
        free(p->query_str);
        p->query_str=NULL;
        return 0;
    }
    free(p->query_str);
    p->query_str=NULL;
    if(validate_query(q, p->tables)!=0)
    {
        delete_query(q);
        return 0;
    }
    if(optimize_query(q, p->tables)!=0)
    {
        delete_query(q);
        return 0;
    }
    bool* bool_array=NULL;
    if(create_sort_array(q, &bool_array)!=0)
    {
        delete_query(q);
        return 0;
    }
    if(optimize_query_memory(q)!=0)
    {
        delete_query(q);
        free(bool_array);
        return 0;
    }
    //    printf("After optimizing memory: ");
    //    print_query_like_an_str(q);
    //Execute
    middleman *middle=execute_query(q, p->tables, bool_array);
    calculate_projections(q, p->tables, middle);
    //Free memory
    for(uint32_t i=0; i<middle->number_of_tables; i++)
    {
        if(middle->tables[i].list!=NULL)
        {
            delete_middle_list(middle->tables[i].list);
        }
    }
    free(middle->tables);
    free(middle);
    free(bool_array);
    delete_query(q);
    return 0;
}
void print_query_job(void* parameters)
{
    job_query_parameters* p=(job_query_parameters*) parameters;
    if(p->query_str==NULL||p->tables==NULL)
    {
        printf("Empty query job");
        return;
    }
    printf("The query %"PRIu64" to analyze: %s\n",p->query_id ,p->query_str);
}
void destroy_query_job(void* parameters)
{
    job_query_parameters* p=(job_query_parameters*) parameters;
    if(p!=NULL&&p->query_str!=NULL)
    {
        free(p->query_str);
        p->query_str=NULL;
    }
    if(p!=NULL)
    {
        free(p);
        p=NULL;
    }
}
