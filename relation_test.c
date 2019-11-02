#include <stdio.h>
#include <stdlib.h>
#include <CUnit/CUnit.h>
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
    char *filename = "./test_files/doesnt_exist.txt";
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
    char *filename = "./test_files/empty_file.txt";
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
    char *filename = "./test_files/fail_table_test_file.txt";
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
    char *filename = "./test_files/empty_table_test_file.txt";
    table* t=read_from_file(filename);
    if(t == NULL || t->rows != 0 || t->array != NULL)
    {
        CU_ASSERT(0);
    }
}

/**
 * Test case 5: Normal file with 4 rows, 3 columns
 */
void testRead_from_file5()
{
    char *filename = "./test_files/small_table_test_file.txt";
    table* t=read_from_file(filename);
    if(t == NULL || t->rows != 3 || t->columns != 4 || t->array == NULL)
    {
        CU_ASSERT(0);
    }

    for(uint64_t i=0; i<3; i++)
    {
    	for(uint64_t j=0; j<4; j++)
    	{
    		if(t->array[i][j] != i+j)
    		{
    			CU_ASSERT(0);
    		}
    	}
    }
}

/**
 * Test case 6: Normal file with 1000 rows, 1000 columns
 */
void testRead_from_file6()
{
    char *filename = "./test_files/big_table_test_file.txt";
    table* t=read_from_file(filename);
    if(t == NULL || t->rows != 1000 || t->columns != 1000 || t->array == NULL)
    {
        CU_ASSERT(0);
    }

    for(uint64_t i=0; i<3; i++)
    {
    	for(uint64_t j=0; j<4; j++)
    	{
    		if(t->array[i][j] != i+j)
    		{
    			CU_ASSERT(0);
    		}
    	}
    }
}

/* No test for delete */
/*
void testDelete_table()
{
    table* p2;
    delete_table(p0);
    if(1)
    {
        CU_ASSERT(0);
    }
}*/


/**
 * Test case 1: key_column NULL
 */
void testCreate_relation_from_table1()
{
    relation* rel = malloc(sizeof(relation));
    rel->tuples = NULL; rel->num_tuples = 0;
    int result=create_relation_from_table(NULL, 1, rel);
    CU_ASSERT_EQUAL(result, 1);
    free(rel);
}

/**
 * Test case 2: relation NULL
 */
void testCreate_relation_from_table2()
{
    uint64_t **array = malloc(3*sizeof(uint64_t *));
    for(uint64_t i=0; i<3; i++)
    {
    	array[i] = malloc(3*sizeof(uint64_t));
    }
    int result=create_relation_from_table(&(array[0][0]), 3, NULL);
    CU_ASSERT_EQUAL(result, 1);
    for(uint64_t i=0; i<3; i++)
    {
    	free(array[i]);
    }
    free(array);
}

/**
 * Test case 3: Relation is full
 */
void testCreate_relation_from_table3()
{
    uint64_t **array = malloc(3*sizeof(uint64_t *));
    for(uint64_t i=0; i<3; i++)
    {
    	array[i] = malloc(3*sizeof(uint64_t));
    }
    relation *rel = malloc(sizeof(relation));
    rel->num_tuples = 5;
    rel->tuples = malloc(5*sizeof(tuple));
    for(uint64_t i=0; i<5; i++)
    {
    	rel->tuples[i].key = i;
    	rel->tuples[i].row_id = 100*i;
    }

    int result=create_relation_from_table(&(array[0][0]), 3, rel);
    CU_ASSERT_EQUAL(result, 1);

	for(uint64_t i=0; i<3; i++)
    {
    	free(array[i]);
    }
    free(array);
    free(rel->tuples);
    free(rel);
}

/**
 * Test case 4: Small table 3x4
 */
void testCreate_relation_from_table4()
{
    uint64_t **array = malloc(3*sizeof(uint64_t *));
    for(uint64_t i=0; i<3; i++)
    {
    	array[i] = malloc(4*sizeof(uint64_t));
    }
    for(uint64_t i=0; i<3; i++)
    {
    	for(uint64_t j=0; j<4; j++)
    	{
    		array[i][j] = i+j;
    	}
    }

    relation *rel = malloc(sizeof(relation));
    rel->tuples = NULL; rel->num_tuples = 0;
    int result=create_relation_from_table(&(array[0][0]), 3, rel);
    if(result != 0)
    {
    	CU_ASSERT(0);
    }

    for(uint64_t i=0; i<3; i++)
    {
    	if(rel->tuples[i].key != i || rel->tuples[i].row_id != i)
    	{
    		CU_ASSERT(0);
    	}
    }

    for(uint64_t i=0; i<3; i++)
    {
    	free(array[i]);
    }
    free(array);
    free(rel->tuples);
    free(rel);
}


/**
 * Test case 5: Big table 1000x1000
 */
void testCreate_relation_from_table5()
{
    uint64_t **array = malloc(1000*sizeof(uint64_t *));
    for(uint64_t i=0; i<1000; i++)
    {
    	array[i] = malloc(1000*sizeof(uint64_t));
    }

    for(uint64_t i=0; i<1000; i++)
    {
    	for(uint64_t j=0; j<1000; j++)
    	{
    		array[i][j] = i+j;
    	}
    }

    relation *rel = malloc(sizeof(relation));
    rel->tuples = NULL; rel->num_tuples = 0;
    int result=create_relation_from_table(&(array[0][0]), 1000, rel);
    if(result != 0)
    {
    	CU_ASSERT(0);
    }

    for(uint64_t i=0; i<1000; i++)
    {
    	if(rel->tuples[i].key != i || rel->tuples[i].row_id != i)
    	{
    		CU_ASSERT(0);
    	}
    }

    for(uint64_t i=0; i<1000; i++)
    {
    	free(array[i]);
    }
    free(array);
    free(rel->tuples);
    free(rel);
}

/**
 * Test case 1: File does not exist
 */
void testRelation_from_file1()
{
	char *filename = "./test_files/doesnt_exist.txt";
	relation *rel=relation_from_file(filename);
	if(rel != NULL)
	{
		CU_ASSERT(0);
	}
}

/**
 * Test case 2: File has only one column
 */
void testRelation_from_file2()
{
	char *filename = "./test_files/fail_relation_from_file.txt";
	relation *rel=relation_from_file(filename);
	if(rel != NULL)
	{
		CU_ASSERT(0);
	}
}

/**
 * Test case 3: Empty file
 */
void testRelation_from_file3()
{
	char *filename = "./test_files/empty_file.txt";
	relation *rel=relation_from_file(filename);
	if(rel != NULL)
	{
	    CU_ASSERT(0);
	}
}

/**
 * Test case 4: Normal file with 5 lines
 */
void testRelation_from_file4()
{
	char *filename = "./test_files/small_relation_test_file.txt";
	relation *rel=relation_from_file(filename);
	if(rel == NULL || rel->num_tuples != 5 || rel->tuples == NULL)
	{
	    CU_ASSERT(0);
	}

	for(uint64_t i=0; i<5; i++)
	{
		if(rel->tuples[i].key != i || rel->tuples[i].row_id != 4-i)
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
	char *filename = "./test_files/big_relation_test_file.txt";
	relation *rel=relation_from_file(filename);
	if(rel == NULL || rel->num_tuples != 1000 || rel->tuples == NULL)
	{
	    CU_ASSERT(0);
	}

	for(uint64_t i=0; i<1000; i++)
	{
		if(rel->tuples[i].key != i || rel->tuples[i].row_id != 999-i)
		{
			CU_ASSERT(0);
		}
	}
}


/* No test for print */
/*
void testPrint_relation()
{
    relation* p2;
    print_relation(p0);
    if(1)
    {
        CU_ASSERT(0);
    }
}
*/

/* No test for print */
/*
void testPrint_tuples()
{
    tuple* t;
    uint64_t items;
    print_tuples(t, items);
    if(1)
    {
        CU_ASSERT(0);
    }
}
*/

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
    if((NULL==CU_add_test(pSuite, "testRead_from_file1", testRead_from_file1))||
    		(NULL==CU_add_test(pSuite, "testRead_from_file2", testRead_from_file2))||
    		(NULL==CU_add_test(pSuite, "testRead_from_file3", testRead_from_file3))||
    		(NULL==CU_add_test(pSuite, "testRead_from_file4", testRead_from_file4))||
    		(NULL==CU_add_test(pSuite, "testRead_from_file5", testRead_from_file5))||
    		(NULL==CU_add_test(pSuite, "testRead_from_file6", testRead_from_file6))||
    		(NULL==CU_add_test(pSuite, "testCreate_relation_from_table1", testCreate_relation_from_table1))||
    		(NULL==CU_add_test(pSuite, "testCreate_relation_from_table2", testCreate_relation_from_table2))||
    		(NULL==CU_add_test(pSuite, "testCreate_relation_from_table3", testCreate_relation_from_table3))||
    		(NULL==CU_add_test(pSuite, "testCreate_relation_from_table4", testCreate_relation_from_table4))||
            (NULL==CU_add_test(pSuite, "testCreate_relation_from_table5", testCreate_relation_from_table5))||
            (NULL==CU_add_test(pSuite, "testRelation_from_file1", testRelation_from_file1))||
            (NULL==CU_add_test(pSuite, "testRelation_from_file2", testRelation_from_file2))||
            (NULL==CU_add_test(pSuite, "testRelation_from_file3", testRelation_from_file3))||
            (NULL==CU_add_test(pSuite, "testRelation_from_file4", testRelation_from_file4))||
            (NULL==CU_add_test(pSuite, "testRelation_from_file5", testRelation_from_file5)))
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
