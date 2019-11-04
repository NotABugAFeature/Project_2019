#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <CUnit/Basic.h>
#include "../sort_merge_join.h"

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


/**
 * Test case 1. Relation t is NULL
 *
 */
void testFinal_join1()
{
    result_list* list = create_result_list();
    relation p2;
    int res = final_join(list, NULL, &p2);

    CU_ASSERT_EQUAL(res, 1);
    delete_result_list(list);
}


/**
 * Test case 2. Relation s is NULL
 *
 */
void testFinal_join2()
{
    result_list* list = create_result_list();
    relation p2;
    int res = final_join(list, &p2, NULL);

    CU_ASSERT_EQUAL(res, 1);
    delete_result_list(list);
}


/**
 * Test case 3. Result list is NULL
 *
 */
void testFinal_join3()
{
    relation p1, p2;
    int res = final_join(NULL, &p1, &p2);

    CU_ASSERT_EQUAL(res, 1);
}


/**
 * Test case 4. Relation t is empty
 *
 */
void testFinal_join4()
{
    result_list* list = create_result_list();

    relation p1, p2;

    p1.num_tuples = 0;
    p1.tuples = NULL;

    p2.num_tuples = 3;
    p2.tuples = malloc(3*sizeof(tuple));
    p2.tuples[0].key = 1;
    p2.tuples[0].row_id = 1;

    p2.tuples[1].key = 1;
    p2.tuples[1].row_id = 2;

    p2.tuples[2].key = 3;
    p2.tuples[2].row_id = 3;


    int res = final_join(list, &p1, &p2);

    CU_ASSERT_EQUAL(res, 1);

    free(p2.tuples);
    delete_result_list(list);
}


/**
 * Test case 5. Relation s is empty
 *
 */
void testFinal_join5()
{
    result_list* list = create_result_list();

    relation p1, p2;

    p1.num_tuples = 0;
    p1.tuples = NULL;

    p2.num_tuples = 3;
    p2.tuples = malloc(3*sizeof(tuple));
    p2.tuples[0].key = 1;
    p2.tuples[0].row_id = 1;

    p2.tuples[1].key = 1;
    p2.tuples[1].row_id = 2;

    p2.tuples[2].key = 3;
    p2.tuples[2].row_id = 3;


    int res = final_join(list, &p2, &p1);

    CU_ASSERT_EQUAL(res, 1);

    free(p2.tuples);
    delete_result_list(list);
}


/**
 * Test case 6. Successful test
 *
 */
void testFinal_join6()
{
    result_list* list = create_result_list();
    
    relation p1, p2;

    p1.num_tuples = 4;
    p1.tuples = malloc(4*sizeof(tuple));
    p1.tuples[0].key = 1;
    p1.tuples[0].row_id = 1;

    p1.tuples[1].key = 2;
    p1.tuples[1].row_id = 2;

    p1.tuples[2].key = 3;
    p1.tuples[2].row_id = 3;

    p1.tuples[3].key = 4;
    p1.tuples[3].row_id = 4;


    p2.num_tuples = 3;
    p2.tuples = malloc(3*sizeof(tuple));
    p2.tuples[0].key = 1;
    p2.tuples[0].row_id = 1;

    p2.tuples[1].key = 1;
    p2.tuples[1].row_id = 2;

    p2.tuples[2].key = 3;
    p2.tuples[2].row_id = 3;


    int res = final_join(list, &p1, &p2);

    CU_ASSERT_EQUAL(res, 0);
    CU_ASSERT_EQUAL(result_list_get_number_of_records(list), 3);

    free(p1.tuples);
    free(p2.tuples);
    delete_result_list(list);
}


/**
 * Test case 7. Successful test
 *
 */
void testFinal_join7()
{
    result_list* list = create_result_list();
    
    relation p1, p2;

    p1.num_tuples = 4;
    p1.tuples = malloc(4*sizeof(tuple));
    p1.tuples[0].key = 6;
    p1.tuples[0].row_id = 1;

    p1.tuples[1].key = 8;
    p1.tuples[1].row_id = 2;

    p1.tuples[2].key = 39;
    p1.tuples[2].row_id = 3;

    p1.tuples[3].key = 46;
    p1.tuples[3].row_id = 4;


    p2.num_tuples = 3;
    p2.tuples = malloc(3*sizeof(tuple));
    p2.tuples[0].key = 1;
    p2.tuples[0].row_id = 1;

    p2.tuples[1].key = 1;
    p2.tuples[1].row_id = 2;

    p2.tuples[2].key = 3;
    p2.tuples[2].row_id = 3;


    int res = final_join(list, &p1, &p2);

    CU_ASSERT_EQUAL(res, 0);
    CU_ASSERT_EQUAL(result_list_get_number_of_records(list), 0);

    free(p1.tuples);
    free(p2.tuples);
    delete_result_list(list);
}


/**
 * Test case 8. Successful test
 *
 */
void testFinal_join8()
{
    result_list* list = create_result_list();
    
    relation p1, p2;

    p1.num_tuples = 4;
    p1.tuples = malloc(4*sizeof(tuple));
    p1.tuples[0].key = 2251799813684550;
    p1.tuples[0].row_id = 1;

    p1.tuples[1].key = 2251799813684730;
    p1.tuples[1].row_id = 2;

    p1.tuples[2].key = 2251799813684860;
    p1.tuples[2].row_id = 3;

    p1.tuples[3].key = 4647856104846919671;
    p1.tuples[3].row_id = 4;


    p2.num_tuples = 3;
    p2.tuples = malloc(3*sizeof(tuple));
    p2.tuples[0].key = 1;
    p2.tuples[0].row_id = 1;

    p2.tuples[1].key = 2251799813684550;
    p2.tuples[1].row_id = 2;

    p2.tuples[2].key = 4647856104846919671;
    p2.tuples[2].row_id = 3;


    int res = final_join(list, &p1, &p2);

    CU_ASSERT_EQUAL(res, 0);
    CU_ASSERT_EQUAL(result_list_get_number_of_records(list), 2);

    free(p1.tuples);
    free(p2.tuples);
    delete_result_list(list);
}


/**
 * Test case 1. relR is NULL
 *
 */
void testSort_merge_join1()
{
    relation relS;
    result_list* result = sort_merge_join(NULL, &relS);
    CU_ASSERT_EQUAL(result, NULL);
}


/**
 * Test case 2. relS is NULL
 *
 */
void testSort_merge_join2()
{
    relation relR;
    result_list* result = sort_merge_join(&relR, NULL);
    CU_ASSERT_EQUAL(result, NULL);
}


/**
 * Test case 3. Successful test with small relations
 *
 */
void testSort_merge_join3()
{
    relation p1, p2;

    p1.num_tuples = 4;
    p1.tuples = malloc(4*sizeof(tuple));
    p1.tuples[0].key = 1;
    p1.tuples[0].row_id = 1;

    p1.tuples[1].key = 2;
    p1.tuples[1].row_id = 2;

    p1.tuples[2].key = 3;
    p1.tuples[2].row_id = 3;

    p1.tuples[3].key = 4;
    p1.tuples[3].row_id = 4;


    p2.num_tuples = 3;
    p2.tuples = malloc(3*sizeof(tuple));
    p2.tuples[0].key = 1;
    p2.tuples[0].row_id = 1;

    p2.tuples[1].key = 1;
    p2.tuples[1].row_id = 2;

    p2.tuples[2].key = 3;
    p2.tuples[2].row_id = 3;

    result_list* result = sort_merge_join(&p1, &p2);

    CU_ASSERT_NOT_EQUAL(result, NULL);
    CU_ASSERT_EQUAL(result_list_get_number_of_records(result), 3);

    free(p1.tuples);
    free(p2.tuples);
    delete_result_list(result);
}


/**
 * Test case 4. Successful test with one small and one big (ordered) relation
 *
 */
void testSort_merge_join4()
{
    relation p1, p2;

    p1.num_tuples = 100000;
    uint64_t init = 2251799813685020;
    p1.tuples = malloc((p1.num_tuples)*sizeof(tuple));
    for(int i = 0; i < p1.num_tuples; i++)
    {
        p1.tuples[i].key = init;
        init++;
        p1.tuples[i].row_id = i;
    }

    p2.num_tuples = 3;
    p2.tuples = malloc(3*sizeof(tuple));
    p2.tuples[0].key = 2251799813685081;
    p2.tuples[0].row_id = 1;

    p2.tuples[1].key = 2251799813685127;
    p2.tuples[1].row_id = 2;

    p2.tuples[2].key = 2251799813685020;
    p2.tuples[2].row_id = 3;

    result_list* result = sort_merge_join(&p1, &p2);

    CU_ASSERT_NOT_EQUAL(result, NULL);
    CU_ASSERT_EQUAL(result_list_get_number_of_records(result), 3);

    free(p1.tuples);
    free(p2.tuples);    
    delete_result_list(result);
}


/**
 * Test case 5. Successful test with one small and one big (reverse order) relation
 *
 */
void testSort_merge_join5()
{
    relation p1, p2;

    p1.num_tuples = 300000;
    uint64_t init = 4647856104846919648;
    p1.tuples = malloc((p1.num_tuples)*sizeof(tuple));
    for(int i = 0; i < p1.num_tuples; i++)
    {
        p1.tuples[i].key = init;
        init--;
        p1.tuples[i].row_id = i;
    }

    p2.num_tuples = 3;
    p2.tuples = malloc(3*sizeof(tuple));
    p2.tuples[0].key = 4647856104846919348;
    p2.tuples[0].row_id = 1;

    p2.tuples[1].key = 4647856104846919148;
    p2.tuples[1].row_id = 2;

    p2.tuples[2].key = 4647856104846911248;
    p2.tuples[2].row_id = 3;

    result_list* result = sort_merge_join(&p1, &p2);

    CU_ASSERT_NOT_EQUAL(result, NULL);
    CU_ASSERT_EQUAL(result_list_get_number_of_records(result), 3);

    free(p1.tuples);
    free(p2.tuples);    
    delete_result_list(result);
}


/**
 * Test case 6. Successful test with two big relations
 *
 */
void testSort_merge_join6()
{
    relation p1, p2;

    p1.num_tuples = 100000;
    uint64_t init = 4647856104846919648;
    p1.tuples = malloc((p1.num_tuples)*sizeof(tuple));
    for(int i = 0; i < p1.num_tuples; i++)
    {
        p1.tuples[i].key = init;
        init--;
        p1.tuples[i].row_id = i;
    }

    p2.num_tuples = 100000;
    init=4647856104846919648;
    p2.tuples = malloc((p2.num_tuples)*sizeof(tuple));
    for(int i = 0; i < p2.num_tuples; i++)
    {
        p2.tuples[i].key = init;
        init--;
        p2.tuples[i].row_id = i;
    }

    result_list* result = sort_merge_join(&p1, &p2);

    CU_ASSERT_NOT_EQUAL(result, NULL);
    CU_ASSERT_EQUAL(result_list_get_number_of_records(result), 100000);

    free(p1.tuples);
    free(p2.tuples);    
    delete_result_list(result);
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
    pSuite=CU_add_suite("sort_merge_join_test", init_suite, clean_suite);
    if(NULL==pSuite)
    {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* Add the tests to the suite */
    if( (NULL==CU_add_test(pSuite, "testFinal_join1", testFinal_join1))||
        (NULL==CU_add_test(pSuite, "testFinal_join2", testFinal_join2))||
        (NULL==CU_add_test(pSuite, "testFinal_join3", testFinal_join3))||
        (NULL==CU_add_test(pSuite, "testFinal_join4", testFinal_join4))||
        (NULL==CU_add_test(pSuite, "testFinal_join5", testFinal_join5))||
        (NULL==CU_add_test(pSuite, "testFinal_join6", testFinal_join6))||
        (NULL==CU_add_test(pSuite, "testFinal_join7", testFinal_join7))||
        (NULL==CU_add_test(pSuite, "testFinal_join8", testFinal_join8))||
        (NULL==CU_add_test(pSuite, "testSort_merge_join1", testSort_merge_join1))||
        (NULL==CU_add_test(pSuite, "testSort_merge_join2", testSort_merge_join2))||
        (NULL==CU_add_test(pSuite, "testSort_merge_join3", testSort_merge_join3))||
        (NULL==CU_add_test(pSuite, "testSort_merge_join4", testSort_merge_join4))||
        (NULL==CU_add_test(pSuite, "testSort_merge_join5", testSort_merge_join5))||
        (NULL==CU_add_test(pSuite, "testSort_merge_join6", testSort_merge_join6)))
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
