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


/**
 * Test case 1: File does not exist
 */
void testRead_from_file1()
{
    char *filename = "/test_files/doesnt_exist.txt";
    table* t=read_from_file(filename);
    if(t != NULL)
    {
        CU_ASSERT(0);
    }
}

/**
 * Test case 2: Empty file
 */
void testRead_from_file2()
{
    char *filename = "/test_files/empty_file.txt";
    table* t=read_from_file(filename);
    if(t != NULL)
    {
        CU_ASSERT(0);
    }
}

/**
 * Test case 3: Wrong column-row numbers
 */
void testRead_from_file3()
{
    char *filename = "/test_files/fail_table_test_file.txt";
    table* t=read_from_file(filename);
    if(t != NULL)
    {
        CU_ASSERT(0);
    }
}

/**
 * Test case 4: 0 rows
 */
void testRead_from_file4()
{
    char *filename = "/test_files/empty_table_test_file.txt";
    table* t=read_from_file(filename);
    if(t == NULL || t->rows != 0 || t->table != NULL)
    {
        CU_ASSERT(0);
    }
}

/**
 * Test case 5: Normal file with 4 rows, 3 columns
 */
void testRead_from_file4()
{
    char *filename = "/test_files/small_table_test_file.txt";
    table* t=read_from_file(filename);
    if(t == NULL || t->rows != 3 || t->columns != 4 || t->table == NULL)
    {
        CU_ASSERT(0);
    }

    for(uint64_t i=0; i<3; i++)
    {
    	for(uint64_t j=0; j<4; j++)
    	{
    		if(t->table[i][j] != i+j)
    		{
    			CU_ASSERT(0);
    		}
    	}
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

/**
 * Test case 1: File does not exist
 */
void testRelation_from_file1()
{
	char *filename = "/test_files/doesnt_exist.txt";
	relation *rel=relation_from_file(filename);
	if(relation != NULL)
	{
		CU_ASSERT(0);
	}
}

/**
 * Test case 2: File has only one column
 */
void testRelation_from_file2()
{
	char *filename = "/test_files/fail_relation_from_file.txt";
	relation *rel=relation_from_file(filename);
	if(relation != NULL)
	{
		CU_ASSERT(0);
	}
}

/**
 * Test case 3: Empty file
 */
void testRelation_from_file3()
{
	char *filename = "/test_files/empty_file.txt";
	relation *rel=relation_from_file(filename);
	if(relation == NULL || relation->num_tuples != 0 || relation->tuples != NULL)
	{
	    CU_ASSERT(0);
	}
}

/**
 * Test case 4: Normal file with 5 lines
 */
void testRelation_from_file4()
{
	char *filename = "/test_files/small_relation_test_file.txt";
	relation *rel=relation_from_file(filename);
	if(relation == NULL || relation->num_tuples != 5 || relation->tuples == NULL)
	{
	    CU_ASSERT(0);
	}

	for(uint64_t i=0; i<5; i++)
	{
		if(relation->tuples[i].key != i || relation->tuples[i].rowId != 4-i)
		{
			CU_ASSERT(0);
		}
	}
}


/**
 * Test case 5: Normal file with 1000 lines
 */
void testRelation_from_file5()
{
	char *filename = "/test_files/big_relation_test_file.txt";
	relation *rel=relation_from_file(filename);
	if(relation == NULL || relation->num_tuples != 1000 || relation->tuples == NULL)
	{
	    CU_ASSERT(0);
	}

	for(uint64_t i=0; i<1000; i++)
	{
		if(relation->tuples[i].key != i || relation->tuples[i].rowId != 999-i)
		{
			CU_ASSERT(0);
		}
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
            (NULL==CU_add_test(pSuite, "testRelation_from_file", testRelation_from_file))||
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
