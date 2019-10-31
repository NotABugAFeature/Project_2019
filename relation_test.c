#include <stdio.h>
#include <stdlib.h>
#include <CUnit/Basic.h>
#include "relation.h"

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

void testRead_from_file()
{
    char* p2;
    table* result=read_from_file(p0);
    if(1 /*check result*/)
    {
        CU_ASSERT(0);
    }
}

void testDelete_table()
{
    table* p2;
    delete_table(p0);
    if(1 /*check result*/)
    {
        CU_ASSERT(0);
    }
}

void testCreate_relation_from_table()
{
    uint64_t* p2;
    uint64_t p3;
    relation* p4;
    int result=create_relation_from_table(p0, p1, p2);
    if(1 /*check result*/)
    {
        CU_ASSERT(0);
    }
}

void testPrint_relation()
{
    relation* p2;
    print_relation(p0);
    if(1 /*check result*/)
    {
        CU_ASSERT(0);
    }
}

void testPrint_tuples()
{
    tuple* t;
    uint64_t items;
    print_tuples(t, items);
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
    pSuite=CU_add_suite("relation_test", init_suite, clean_suite);
    if(NULL==pSuite)
    {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* Add the tests to the suite */
    if((NULL==CU_add_test(pSuite, "testRead_from_file", testRead_from_file))||
            (NULL==CU_add_test(pSuite, "testDelete_table", testDelete_table))||
            (NULL==CU_add_test(pSuite, "testCreate_relation_from_table", testCreate_relation_from_table))||
            (NULL==CU_add_test(pSuite, "testPrint_relation", testPrint_relation))||
            (NULL==CU_add_test(pSuite, "testPrint_tuples", testPrint_tuples)))
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
