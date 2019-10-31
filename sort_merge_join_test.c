/*
 * File:   sort_merge_join_test.c
 * Author: userp
 *
 * Created on 31 Οκτ 2019, 5:47:51 μμ
 */

#include <stdio.h>
#include <stdlib.h>
#include <CUnit/Basic.h>
#include "sort_merge_join.h"

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

void testFinal_join()
{
    result_list* p2;
    relation* p3;
    relation* p4;
    final_join(p0, p1, p2);
    if(1 /*check result*/)
    {
        CU_ASSERT(0);
    }
}

void testSort_merge_join()
{
    relation* relR;
    relation* relS;
    result_list* result=sort_merge_join(relR, relS);
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
    pSuite=CU_add_suite("sort_merge_join_test", init_suite, clean_suite);
    if(NULL==pSuite)
    {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* Add the tests to the suite */
    if((NULL==CU_add_test(pSuite, "testFinal_join", testFinal_join))||
            (NULL==CU_add_test(pSuite, "testSort_merge_join", testSort_merge_join)))
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
