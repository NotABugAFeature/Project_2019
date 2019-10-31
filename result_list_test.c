/*
 * File:   result_list_test.c
 * Author: userp
 *
 * Created on 31 Οκτ 2019, 5:47:06 μμ
 */

#include <stdio.h>
#include <stdlib.h>
#include <CUnit/Basic.h>
#include "result_list.h"

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

result_list_node* create_result_list_node();

void testCreate_result_list_node()
{
    result_list_node* result=create_result_list_node();
    if(1 /*check result*/)
    {
        CU_ASSERT(0);
    }
}

int is_result_list_bucket_full(result_list_bucket* bucket);

void testIs_result_list_bucket_full()
{
    result_list_bucket* bucket;
    int result=is_result_list_bucket_full(bucket);
    if(1 /*check result*/)
    {
        CU_ASSERT(0);
    }
}

int append_to_bucket(result_list_bucket* bucket, uint64_t r_row_id, uint64_t s_row_id);

void testAppend_to_bucket()
{
    result_list_bucket* bucket;
    uint64_t r_row_id;
    uint64_t s_row_id;
    int result=append_to_bucket(bucket, r_row_id, s_row_id);
    if(1 /*check result*/)
    {
        CU_ASSERT(0);
    }
}

void print_bucket(result_list_bucket* bucket);

void testPrint_bucket()
{
    result_list_bucket* bucket;
    print_bucket(bucket);
    if(1 /*check result*/)
    {
        CU_ASSERT(0);
    }
}

void testCreate_result_list()
{
    result_list* result=create_result_list();
    if(1 /*check result*/)
    {
        CU_ASSERT(0);
    }
}

void testDelete_result_list()
{
    result_list* p2;
    delete_result_list(p0);
    if(1 /*check result*/)
    {
        CU_ASSERT(0);
    }
}

void testPrint_result_list()
{
    result_list* p2;
    print_result_list(p0);
    if(1 /*check result*/)
    {
        CU_ASSERT(0);
    }
}

void testAppend_to_list()
{
    result_list* p2;
    uint64_t r_row_id;
    uint64_t s_row_id;
    int result=append_to_list(p0, r_row_id, s_row_id);
    if(1 /*check result*/)
    {
        CU_ASSERT(0);
    }
}

void testIs_result_list_empty()
{
    result_list* list;
    int result=is_result_list_empty(list);
    if(1 /*check result*/)
    {
        CU_ASSERT(0);
    }
}

void testResult_list_get_number_of_buckets()
{
    result_list* p2;
    int result=result_list_get_number_of_buckets(p0);
    if(1 /*check result*/)
    {
        CU_ASSERT(0);
    }
}

int main()
{
    CU_pSuite pSuite=NULL;

    /* Initialize the CUnit test registry */
    if(CUE_SUCCESS!=CU_initialize_registry())
        return CU_get_error();

    /* Add a suite to the registry */
    pSuite=CU_add_suite("result_list_test", init_suite, clean_suite);
    if(NULL==pSuite)
    {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* Add the tests to the suite */
    if((NULL==CU_add_test(pSuite, "testCreate_result_list_node", testCreate_result_list_node))||
            (NULL==CU_add_test(pSuite, "testIs_result_list_bucket_full", testIs_result_list_bucket_full))||
            (NULL==CU_add_test(pSuite, "testAppend_to_bucket", testAppend_to_bucket))||
            (NULL==CU_add_test(pSuite, "testPrint_bucket", testPrint_bucket))||
            (NULL==CU_add_test(pSuite, "testCreate_result_list", testCreate_result_list))||
            (NULL==CU_add_test(pSuite, "testDelete_result_list", testDelete_result_list))||
            (NULL==CU_add_test(pSuite, "testPrint_result_list", testPrint_result_list))||
            (NULL==CU_add_test(pSuite, "testAppend_to_list", testAppend_to_list))||
            (NULL==CU_add_test(pSuite, "testIs_result_list_empty", testIs_result_list_empty))||
            (NULL==CU_add_test(pSuite, "testResult_list_get_number_of_buckets", testResult_list_get_number_of_buckets)))
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
