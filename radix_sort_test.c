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


void testCreate_histogram1()
{
    relation p2;

    p2.num_tuples = 3;
    p2.tuples = malloc(3*sizeof(tuple));
    p2.tuples[0].key = 1;
    p2.tuples[0].row_id = 1;

    p2.tuples[1].key = 1;
    p2.tuples[1].row_id = 2;

    p2.tuples[2].key = 3;
    p2.tuples[2].row_id = 3;
    
    uint64_t *hist = malloc(HIST_SIZE*sizeof(uint64_t));
    CU_ASSERT_NOT_EQUAL(hist, NULL);

    for(uint64_t i=0; i<HIST_SIZE; i++)
    {
        hist[i] = 0;
    }
    
    int res = create_histogram(&p2, 0, 3, hist, 8);

    CU_ASSERT_EQUAL(res, 0);

    CU_ASSERT_EQUAL(hist[1], 2);
    CU_ASSERT_EQUAL(hist[3], 1);
    
    CU_ASSERT_EQUAL(hist[0], 0);
    CU_ASSERT_EQUAL(hist[2], 0);
    
    for(uint64_t i=4; i<HIST_SIZE; i++)
        CU_ASSERT_EQUAL(hist[i], 0);
    
    free(p2.tuples);
    free(hist);
}


void testCreate_histogram2()
{
    relation *p2;

    uint64_t *hist = malloc(HIST_SIZE*sizeof(uint64_t));
    CU_ASSERT_NOT_EQUAL(hist, NULL);
    
    int res = create_histogram(NULL, 0, 3, hist, 8);
    
    CU_ASSERT_EQUAL(res, 1);

    free(hist);
}


void testCreate_histogram3()
{
    relation *p2;
    
    int res = create_histogram(p2, 0, 3, NULL, 8);
    
    CU_ASSERT_EQUAL(res, 1);
}


void testCreate_histogram4()
{
    relation *p2;
    
    int res = create_histogram(p2, 0, 3, NULL, 8);
    
    CU_ASSERT_EQUAL(res, 1);
}


void testCreate_histogram5()
{
    relation p2;

    p2.num_tuples = 3;
    p2.tuples = malloc(3*sizeof(tuple));
    p2.tuples[0].key = 1;
    p2.tuples[0].row_id = 1;

    p2.tuples[1].key = 1;
    p2.tuples[1].row_id = 2;

    p2.tuples[2].key = 3;
    p2.tuples[2].row_id = 3;
    
    uint64_t *hist = malloc(HIST_SIZE*sizeof(uint64_t));
    CU_ASSERT_NOT_EQUAL(hist, NULL);

    for(uint64_t i=0; i<HIST_SIZE; i++)
    {
        hist[i] = 0;
    }
    
    int res = create_histogram(&p2, 0, 3, hist, 1);

    CU_ASSERT_EQUAL(res, 0);

    CU_ASSERT_EQUAL(hist[0], 3);
    
    for(uint64_t i=1; i<HIST_SIZE; i++)
        CU_ASSERT_EQUAL(hist[i], 0);
    
    free(p2.tuples);
    free(hist);
}


void testTransform_to_psum1()
{
    int res = transform_to_psum(NULL);
    CU_ASSERT_EQUAL(res, 1);
}


void testTransform_to_psum2()
{
    relation p2;

    p2.num_tuples = 3;
    p2.tuples = malloc(3*sizeof(tuple));
    p2.tuples[0].key = 1;
    p2.tuples[0].row_id = 1;

    p2.tuples[1].key = 1;
    p2.tuples[1].row_id = 2;

    p2.tuples[2].key = 3;
    p2.tuples[2].row_id = 3;
    
    uint64_t *hist = malloc(HIST_SIZE*sizeof(uint64_t));
    CU_ASSERT_NOT_EQUAL(hist, NULL);

    for(uint64_t i=0; i<HIST_SIZE; i++)
    {
        hist[i] = 0;
    }
    
    int res = create_histogram(&p2, 0, 3, hist, 8);
    CU_ASSERT_EQUAL(res, 0);

    res = transform_to_psum(hist);
    CU_ASSERT_EQUAL(res, 0);

    CU_ASSERT_EQUAL(hist[0], 0);
    CU_ASSERT_EQUAL(hist[1], 0);
    CU_ASSERT_EQUAL(hist[2], 2);
    CU_ASSERT_EQUAL(hist[3], 2);  
    CU_ASSERT_EQUAL(hist[4], 3);
    
    for(uint64_t i=5; i<HIST_SIZE; i++)
        CU_ASSERT_EQUAL(hist[i], 3);
    
    free(p2.tuples);
    free(hist);
}

/*
void copy_relation(relation* source, relation* target, uint64_t start_index, uint64_t end_index);

void testCopy_relation()
{
    relation* source;
    relation* target;
    uint64_t start_index;
    uint64_t end_index;
    copy_relation(source, target, start_index, end_index);
    if(1)
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
    if(1)
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
    if(1 )
    {
        CU_ASSERT(0);
    }
}
*/
void testRadix_sort1()
{
    relation p2;

    p2.num_tuples = 3;
    p2.tuples = malloc(3*sizeof(tuple));
    p2.tuples[0].key = 4;
    p2.tuples[0].row_id = 1;

    p2.tuples[1].key = 1;
    p2.tuples[1].row_id = 2;

    p2.tuples[2].key = 3;
    p2.tuples[2].row_id = 3;
    
    int res = radix_sort(&p2);
    
    CU_ASSERT_EQUAL(res, 0);
    CU_ASSERT_EQUAL(p2.tuples[0].key, 1);
    CU_ASSERT_EQUAL(p2.tuples[1].key, 3);
    CU_ASSERT_EQUAL(p2.tuples[2].key, 4);

    free(p2.tuples);
}


int main(void)
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
    if( (NULL==CU_add_test(pSuite, "testCreate_histogram1", testCreate_histogram1))||
        (NULL==CU_add_test(pSuite, "testCreate_histogram2", testCreate_histogram2))||
        (NULL==CU_add_test(pSuite, "testCreate_histogram3", testCreate_histogram3))||
        (NULL==CU_add_test(pSuite, "testCreate_histogram4", testCreate_histogram4))||
        (NULL==CU_add_test(pSuite, "testCreate_histogram5", testCreate_histogram5))||
        (NULL==CU_add_test(pSuite, "testTransform_to_psum1", testTransform_to_psum1))||
        (NULL==CU_add_test(pSuite, "testTransform_to_psum2", testTransform_to_psum2))||
        (NULL==CU_add_test(pSuite, "testRadix_sort1", testRadix_sort1))
      )
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
