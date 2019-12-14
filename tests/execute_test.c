#include <stdio.h>
#include <stdlib.h>
#include <CUnit/Basic.h>
#include <string.h>
#include "../execute_query.h"

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

void testExecute_query()
{
    table_index *ti=malloc(sizeof(table_index));
    CU_ASSERT_NOT_EQUAL_FATAL(ti,NULL);
    ti->num_tables=14;
    ti->tables=malloc(sizeof(table)*ti->num_tables);
    if(ti->tables==NULL)
    {
        free(ti);
        CU_ASSERT(0);
        return;
    }
    char path[40];
    bool errors=false;
    unsigned int error_index=0;
    for(unsigned int i=0;i<ti->num_tables;i++)
    {
        snprintf(path,40,"./test_files/r%u",i);
        if(table_from_file(&(ti->tables[i]),path)!=0)
        {
            errors=true;
            if(i==0)
            {
                error_index=0;
            }
            else
            {
                error_index=i+1;
            }
        }
    }
    if(errors)
    {
        ti->num_tables=error_index;
        delete_table_index(ti);
        CU_ASSERT(0);
        return;
    }
    query* q1=create_query();query* q2=create_query();query* q3=create_query();
    query* q4=create_query();query* q5=create_query();query* q6=create_query();
    query* q7=create_query();query* q8=create_query();query* q9=create_query();
    query* q10=create_query();query* q11=create_query();query* q12=create_query();
    query* q13=create_query();query* q14=create_query();query* q15=create_query();
    if(q1==NULL||q2==NULL||q3==NULL||q4==NULL||q5==NULL||q6==NULL||q7==NULL||
       q8==NULL||q9==NULL||q10==NULL||q11==NULL||q12==NULL||q13==NULL||
       q14==NULL||q15==NULL)
    {
        delete_table_index(ti);
        CU_ASSERT(0);
        return;
    }
    char query_str1[200];char query_str2[200];char query_str3[200];
    char query_str4[200];char query_str5[200];char query_str6[200];
    char query_str7[200];char query_str8[200];char query_str9[200];
    char query_str10[200];char query_str11[200];char query_str12[200];
    char query_str13[200];char query_str14[200];char query_str15[200];
    strncpy(query_str1, "1 3 2|1.2=0.1|1.3 1 1.2", 199);
    strncpy(query_str2, " ", 199);
    strncpy(query_str3, " ", 199);
    strncpy(query_str4, " ", 199);
    strncpy(query_str5, " ", 199);
    strncpy(query_str6, " ", 199);
    strncpy(query_str7, " ", 199);
    strncpy(query_str8, " ", 199);
    strncpy(query_str9, " ", 199);
    strncpy(query_str10, " ", 199);
    strncpy(query_str11, " ", 199);
    strncpy(query_str12, " ", 199);
    strncpy(query_str13, " ", 199);
    strncpy(query_str14, " ", 199);
    strncpy(query_str15, " ", 199);
    bool* bool_array=NULL;
    if(analyze_query(q1, query_str1)==0&&validate_query(q1, ti)==0&&
       optimize_query(q1, ti)==0&&create_sort_array(q1, &bool_array)==0&&
       optimize_query_memory(q1)==0
       )
    {
        middleman *middle=execute_query(q1, ti, bool_array);
        calculate_projections(q1, ti, middle);
        //Free memory
        for(uint32_t i=0; i<middle->number_of_tables; i++)
        {
            if(middle->tables[i].list!=NULL)
                delete_middle_list(middle->tables[i].list);
        }
        free(middle->tables);
        free(middle);
        free(bool_array);
        bool_array=NULL;
    }
    else
    {
        CU_ASSERT(1);
    }

    
    delete_query(q1);delete_query(q2);delete_query(q3);delete_query(q4);
    delete_query(q5);delete_query(q6);delete_query(q7);delete_query(q8);
    delete_query(q9);delete_query(q10);delete_query(q11);delete_query(q12);
    delete_query(q13);delete_query(q14);delete_query(q15);
    delete_table_index(ti);
//    return 0;
}

int main()
{
    CU_pSuite pSuite=NULL;

    /* Initialize the CUnit test registry */
    if(CUE_SUCCESS!=CU_initialize_registry())
        return CU_get_error();

    /* Add a suite to the registry */
    pSuite=CU_add_suite("Execute_Test", init_suite, clean_suite);
    if(NULL==pSuite)
    {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* Add the tests to the suite */
    if((NULL==CU_add_test(pSuite, "testExecute_query", testExecute_query)))
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
