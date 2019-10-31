#include <stdio.h>
#include <stdlib.h>
#include <CUnit/Basic.h>
#include "quicksort.h"

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

void testSwap()
{
    tuple* p2;
    tuple* p3;
    swap(p0, p1);
    if(1 /*check result*/)
    {
        CU_ASSERT(0);
    }
}

void testPartition()
{
    tuple* p2;
    uint64_t p3;
    uint64_t p4;
    uint64_t result=partition(p0, p1, p2);
    if(1 /*check result*/)
    {
        CU_ASSERT(0);
    }
}

void testQuicksort()
{
    tuple* p2;
    uint64_t p3;
    uint64_t p4;
    quicksort(p0, p1, p2);
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
    {
        return CU_get_error();
    }
    /* Add a suite to the registry */
    pSuite=CU_add_suite("quicksort_test", init_suite, clean_suite);
    if(NULL==pSuite)
    {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* Add the tests to the suite */
    if((NULL==CU_add_test(pSuite, "testSwap", testSwap))||
            (NULL==CU_add_test(pSuite, "testPartition", testPartition))||
            (NULL==CU_add_test(pSuite, "testQuicksort", testQuicksort)))
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
