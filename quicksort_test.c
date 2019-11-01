#include <stdio.h>
#include <stdlib.h>
#include <CUnit/Basic.h>
#include "../quicksort.h"

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
    //Create two tuples
    tuple t1;
    t1.key=1;
    t1.row_id=101;
    tuple t2;
    t2.key=2;
    t2.row_id=202;
    //Swap them
    swap(&t1, &t2);
    //Check their values
    if(t1.key!=2||t1.row_id!=202||t2.key!=1||t2.row_id!=101)
    {
        CU_ASSERT(0);
    }
}

void testPartition()
{
    int size_of_test=10;
    //Create an array of tuples
    tuple test_tuples[size_of_test];
    //Initialize it with descending values
    for(int i=0;i<10;i++)
    {
        test_tuples[i].key=size_of_test-i;
        test_tuples[i].row_id=10*size_of_test-i;
    }
    print_tuples(test_tuples,size_of_test);
    //Call partition for the array
    uint64_t result=partition(test_tuples, 0, size_of_test-1);
    print_tuples(test_tuples,size_of_test);
    //Check if the first element has swapped with last and the result is 0
    if(result!=0||test_tuples[0].key!=1||test_tuples[size_of_test-1].key!=10)
    {
        CU_ASSERT(0);
    }
    //Re-Initialize it with alternating small and big values
    for(int i=0;i<size_of_test;i++)
    {
        if(i%2==0)
        {
            test_tuples[i].key=size_of_test-i;
            test_tuples[i].row_id=10*size_of_test-i;
        }
        else
        {
            test_tuples[i].key=5000*size_of_test-i;
            test_tuples[i].row_id=50000*size_of_test-i;
        }
    }
    print_tuples(test_tuples,size_of_test);
    //Call partition for the array
    result=partition(test_tuples, 0,size_of_test-1);
    print_tuples(test_tuples,size_of_test);
    //Check if the tuples from 0 to result are < tuple[result]
    for(int i=0;i<result;i++)
    {
        if(test_tuples[i].key>test_tuples[result].key)
        {
            CU_ASSERT(0);
        }
    }
    //Check if the tuples from result to end are > tuple[result]
    for(int i=result;i<size_of_test-1;i++)
    {
        if(test_tuples[i].key<test_tuples[result].key)
        {
            CU_ASSERT(0);
        }
    }
}

void testQuicksort()
{
    int size_of_test=10;
    //Create an array of tuples
    tuple test_tuples[size_of_test];
    //Initialize it with descending values
    for(int i=0;i<10;i++)
    {
        test_tuples[i].key=size_of_test-i;
        test_tuples[i].row_id=10*size_of_test-i;
    }
    print_tuples(test_tuples,size_of_test);
    //Call quicksort for the array
    quicksort(test_tuples, 0, size_of_test-1);
    print_tuples(test_tuples,size_of_test);
    //Check if the array is sorted
    for(int i=0;i<size_of_test-2;i++)
    {
        if(test_tuples[i].key>test_tuples[i+1].key)
        {
            CU_ASSERT(0);
        }
    }
    //Re-Initialize it with alternating small and big values
    for(int i=0;i<size_of_test;i++)
    {
        if(i%2==0)
        {
            test_tuples[i].key=size_of_test-i;
            test_tuples[i].row_id=10*size_of_test-i;
        }
        else
        {
            test_tuples[i].key=5000*size_of_test-i;
            test_tuples[i].row_id=50000*size_of_test-i;
        }
    }
    print_tuples(test_tuples,size_of_test);
    //Call quicksort for the array
    quicksort(test_tuples, 0,size_of_test-1);
    print_tuples(test_tuples,size_of_test);
    //Check if the array is sorted
    for(int i=0;i<size_of_test-2;i++)
    {
        if(test_tuples[i].key>test_tuples[i+1].key)
        {
            CU_ASSERT(0);
        }
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
