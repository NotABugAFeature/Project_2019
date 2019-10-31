#include <stdio.h>
#include <stdlib.h>
#include <CUnit/Basic.h>
#include "radix_sort.h"

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

void testCreate_histogram()
{
    relation* r;
    uint64_t start_index;
    uint64_t end_index;
    uint64_t* hist;
    unsigned short byte_number;
    create_histogram(r, start_index, end_index, hist, byte_number);
    if(1 /*check result*/)
    {
        CU_ASSERT(0);
    }
}

void testTransform_to_psum()
{
    uint64_t* hist;
    transform_to_psum(hist);
    if(1 /*check result*/)
    {
        CU_ASSERT(0);
    }
}

void copy_relation(relation* source, relation* target, uint64_t start_index, uint64_t end_index);

void testCopy_relation()
{
    relation* source;
    relation* target;
    uint64_t start_index;
    uint64_t end_index;
    copy_relation(source, target, start_index, end_index);
    if(1 /*check result*/)
    {
        CU_ASSERT(0);
    }
}

void testCopy_relation_with_psum()
{
    relation* source;
    relation* target;
    uint64_t index_start;
    uint64_t index_end;
    uint64_t* psum;
    unsigned short nbyte;
    int result=copy_relation_with_psum(source, target, index_start, index_end, psum, nbyte);
    if(1 /*check result*/)
    {
        CU_ASSERT(0);
    }
}

void testRadix_sort_recursive()
{
    unsigned short byte;
    relation* array;
    relation* auxiliary;
    uint64_t start_index;
    uint64_t end_index;
    int result=radix_sort_recursive(byte, array, auxiliary, start_index, end_index);
    if(1 /*check result*/)
    {
        CU_ASSERT(0);
    }
}

void testRadix_sort()
{
    relation* array;
    int result=radix_sort(array);
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
    pSuite=CU_add_suite("radix_sort_test", init_suite, clean_suite);
    if(NULL==pSuite)
    {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* Add the tests to the suite */
    if((NULL==CU_add_test(pSuite, "testCreate_histogram", testCreate_histogram))||
            (NULL==CU_add_test(pSuite, "testTransform_to_psum", testTransform_to_psum))||
            (NULL==CU_add_test(pSuite, "testCopy_relation", testCopy_relation))||
            (NULL==CU_add_test(pSuite, "testCopy_relation_with_psum", testCopy_relation_with_psum))||
            (NULL==CU_add_test(pSuite, "testRadix_sort_recursive", testRadix_sort_recursive))||
            (NULL==CU_add_test(pSuite, "testRadix_sort", testRadix_sort)))
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
