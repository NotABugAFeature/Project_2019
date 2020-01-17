#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <CUnit/Basic.h>
#include "../job_fifo.h"
#include "../job_scheduler.h"
/*
 * CUnit Test Suite
 */

int init_suite(void)
{
    return 0;
}
int clean_suite(void)
{
    return 0;
}
void testCreate_job_fifo_node()
{
    job_fifo_node* node=create_job_fifo_node();
    CU_ASSERT_NOT_EQUAL_FATAL(node,NULL);
    CU_ASSERT_EQUAL(node->next,NULL);
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE; i++)
    {
        CU_ASSERT_EQUAL(node->bucket.jobs[i],NULL);
    }
    CU_ASSERT_EQUAL(node->bucket.index_to_add_next,0);
    CU_ASSERT_EQUAL(node->bucket.index_to_remove_next,0);
    free(node);
}
void testCreate_job_fifo()
{
    job_fifo* fifo=create_job_fifo();
    CU_ASSERT_NOT_EQUAL_FATAL(fifo,NULL);
    CU_ASSERT_EQUAL(fifo->head,NULL);
    CU_ASSERT_EQUAL(fifo->append_node,NULL);
    CU_ASSERT_EQUAL(fifo->tail,NULL);
    CU_ASSERT_EQUAL(fifo->number_of_nodes,0);
    CU_ASSERT_EQUAL(fifo->number_of_jobs,0);
    free(fifo);
}
void testAppend_to_job_fifo_bucket()
{
    job_fifo_node* n=create_job_fifo_node();
    job jobs[JOB_FIFO_BUCKET_SIZE+1];
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE; i++)
    {
        CU_ASSERT_EQUAL(append_to_job_fifo_bucket(&n->bucket,&jobs[i]),0);
    }
    CU_ASSERT_EQUAL(append_to_job_fifo_bucket(&n->bucket,&jobs[JOB_FIFO_BUCKET_SIZE]),1);
    
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE; i++)
    {
        CU_ASSERT_EQUAL(n->bucket.jobs[i],&jobs[i]);
    }
}
void testIs_job_fifo_bucket_full()
{
    job_fifo_node* n=create_job_fifo_node();
    job j;
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE; i++)
    {
        CU_ASSERT_EQUAL(is_job_fifo_bucket_full(&n->bucket),false);
        CU_ASSERT_EQUAL(append_to_job_fifo_bucket(&n->bucket,&j),0);
    }
    CU_ASSERT_EQUAL(is_job_fifo_bucket_full(&n->bucket),true);
    free(n);
}
void testAppend_Pop_job_fifo()
{
    job_fifo* fifo=create_job_fifo();
    job jobs[5*JOB_FIFO_BUCKET_SIZE];
    CU_ASSERT_EQUAL(is_job_fifo_empty(fifo),true);
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE*5; i++)
    {
        CU_ASSERT_EQUAL(append_to_job_fifo(fifo,&jobs[i]),0);
        CU_ASSERT_EQUAL(is_job_fifo_empty(fifo),false);
    }
    CU_ASSERT_EQUAL(fifo->number_of_jobs,JOB_FIFO_BUCKET_SIZE*5);
    CU_ASSERT_EQUAL(fifo->number_of_nodes,5);
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE*5; i++)
    {
        CU_ASSERT_EQUAL(is_job_fifo_empty(fifo),false);
        CU_ASSERT_EQUAL(pop_from_job_fifo(fifo),&jobs[i]);
    }
    CU_ASSERT_EQUAL(is_job_fifo_empty(fifo),true);
    CU_ASSERT_EQUAL(fifo->number_of_jobs,0);
    CU_ASSERT_EQUAL(fifo->number_of_nodes,2);
    CU_ASSERT_EQUAL(fifo->head,fifo->append_node);
    CU_ASSERT_NOT_EQUAL(fifo->head,fifo->tail);
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE*3; i++)
    {
        CU_ASSERT_EQUAL(append_to_job_fifo(fifo,&jobs[i]),0);
        CU_ASSERT_EQUAL(is_job_fifo_empty(fifo),false);
    }
    CU_ASSERT_EQUAL(fifo->number_of_jobs,JOB_FIFO_BUCKET_SIZE*3);
    CU_ASSERT_EQUAL(fifo->number_of_nodes,3);
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE*2; i++)
    {
        CU_ASSERT_EQUAL(is_job_fifo_empty(fifo),false);
        CU_ASSERT_EQUAL(pop_from_job_fifo(fifo),&jobs[i]);
    }
    CU_ASSERT_EQUAL(fifo->number_of_jobs,JOB_FIFO_BUCKET_SIZE);
    CU_ASSERT_EQUAL(fifo->number_of_nodes,2);
    CU_ASSERT_EQUAL(fifo->head,fifo->append_node);
    CU_ASSERT_NOT_EQUAL(fifo->head,fifo->tail);
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE*2; i++)
    {
        CU_ASSERT_EQUAL(append_to_job_fifo(fifo,&jobs[i]),0);
        CU_ASSERT_EQUAL(is_job_fifo_empty(fifo),false);
    }
    CU_ASSERT_EQUAL(fifo->number_of_jobs,JOB_FIFO_BUCKET_SIZE*3);
    CU_ASSERT_EQUAL(fifo->number_of_nodes,3);
    CU_ASSERT_EQUAL(fifo->tail,fifo->append_node);
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE; i++)
    {
        CU_ASSERT_EQUAL(pop_from_job_fifo(fifo),&jobs[i+JOB_FIFO_BUCKET_SIZE*2]);
    }
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE*2; i++)
    {
        CU_ASSERT_EQUAL(pop_from_job_fifo(fifo),&jobs[i]);
    }
    CU_ASSERT_EQUAL(is_job_fifo_empty(fifo),true);
    CU_ASSERT_EQUAL(fifo->number_of_jobs,0);
    CU_ASSERT_EQUAL(fifo->number_of_nodes,2);
    CU_ASSERT_EQUAL(fifo->head,fifo->append_node);
    CU_ASSERT_NOT_EQUAL(fifo->head,fifo->tail);
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE*5; i++)
    {
        CU_ASSERT_EQUAL(append_to_job_fifo(fifo,&jobs[i]),0);
        CU_ASSERT_EQUAL(pop_from_job_fifo(fifo),&jobs[i]);
        CU_ASSERT_EQUAL(is_job_fifo_empty(fifo),true);
        CU_ASSERT_EQUAL(fifo->number_of_jobs,0);
        CU_ASSERT_EQUAL(fifo->number_of_nodes,2);
        CU_ASSERT_EQUAL(fifo->head,fifo->append_node);
    }
    delete_job_fifo(fifo);
}
int main()
{
    CU_pSuite pSuite=NULL;

    /* Initialize the CUnit test registry */
    if(CUE_SUCCESS!=CU_initialize_registry())
        return CU_get_error();

    /* Add a suite to the registry */
    pSuite=CU_add_suite("job_fifo", init_suite, clean_suite);
    if(NULL==pSuite)
    {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* Add the tests to the suite */
    if((NULL==CU_add_test(pSuite, "testCreate_job_fifo_node", testCreate_job_fifo_node))||
       (NULL==CU_add_test(pSuite, "testIs_job_fifo_bucket_full", testIs_job_fifo_bucket_full))||
       (NULL==CU_add_test(pSuite, "testAppend_to_job_fifo_bucket", testAppend_to_job_fifo_bucket))||
       (NULL==CU_add_test(pSuite, "testCreate_job_fifo", testCreate_job_fifo))||
       (NULL==CU_add_test(pSuite, "testAppend_to_job_fifo", testAppend_Pop_job_fifo)))
    {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* Run all tests using the CUnit Basic interface */
    CU_basic_set_mode(CU_BRM_VERBOSE);
    CU_basic_run_tests();
    CU_cleanup_registry();
    return CU_get_error();
}
