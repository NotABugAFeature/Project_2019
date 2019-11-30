#include <stdio.h>
#include <stdlib.h>
#include <CUnit/Basic.h>
#include "../query.h"
#include <limits.h>

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
void remove_extra_chars(char*, char);

void testRemove_extra_chars()
{
    char query_str1[10];
    strncpy(query_str1, "         ", 10);
    remove_extra_chars(query_str1, ' ');
    CU_ASSERT_EQUAL(strcmp(query_str1, ""), 0);
    char query_str2[20];
    strncpy(query_str2, "         end", 20);
    remove_extra_chars(query_str2, ' ');
    CU_ASSERT_EQUAL(strcmp(query_str2, "end"), 0);
    char query_str3[20];
    strncpy(query_str3, "         end      ", 20);
    remove_extra_chars(query_str3, ' ');
    CU_ASSERT_EQUAL(strcmp(query_str3, "end"), 0);
    char query_str4[20];
    strncpy(query_str4, "start      ", 20);
    remove_extra_chars(query_str4, ' ');
    CU_ASSERT_EQUAL(strcmp(query_str4, "start"), 0);
    char query_str5[20];
    strncpy(query_str5, "start      end", 20);
    remove_extra_chars(query_str5, ' ');
    CU_ASSERT_EQUAL(strcmp(query_str5, "start end"), 0)
            char query_str6[30];
    strncpy(query_str6, "     start      end     ", 30);
    remove_extra_chars(query_str6, ' ');
    CU_ASSERT_EQUAL(strcmp(query_str6, "start end"), 0)
            char query_str7[40];
    strncpy(query_str7, "start    middle     end", 40);
    remove_extra_chars(query_str7, ' ');
    CU_ASSERT_EQUAL(strcmp(query_str7, "start middle end"), 0)
            char query_str8[40];
    strncpy(query_str8, "   start    middle     end  ", 40);
    remove_extra_chars(query_str8, ' ');
    CU_ASSERT_EQUAL(strcmp(query_str8, "start middle end"), 0)
            char query_str9[40];
    strncpy(query_str9, " 12323 11     455 121    12222       ", 40);
    remove_extra_chars(query_str9, ' ');
    CU_ASSERT_EQUAL(strcmp(query_str9, "12323 11 455 121 12222"), 0)
            char query_str10[50];
    strncpy(query_str10, "__ 12323 11_______ 455 _ 121 __ 12222 ____ ", 50);
    remove_extra_chars(query_str10, '_');
    CU_ASSERT_EQUAL(strcmp(query_str10, " 12323 11_ 455 _ 121 _ 12222 _ "), 0)
}

/**
 * Test 1 Null char*, Null query* and then not empty queries
 * The function must return !=0
 */
void testAnalyze_query1()
{
    char* query_str_null=NULL;
    query* q1=create_query();
    if(q1!=NULL)
    {
        CU_ASSERT_NOT_EQUAL(analyze_query(query_str_null, q1), 0);
        delete_query(q1);
    }
    char* query_str="Wrong Str";
    query* q=NULL;
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str, q), 0);
    q=create_query();
    print_query(q);
    q->number_of_tables=1;
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str, q), 0);
    q->number_of_tables=0;
    q->number_of_predicates=1;
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str, q), 0);
    q->number_of_predicates=0;
    q->number_of_projections=1;
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str, q), 0);
    q->number_of_projections=0;
    q->table_ids=malloc(2*sizeof(unsigned int));
    if(q->table_ids==NULL)
    {
        perror("error in malloc");
    }
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str, q), 0);
    free(q->table_ids);
    q->table_ids=NULL;
    q->predicates=malloc(2*sizeof(predicate));
    if(q->predicates==NULL)
    {
        perror("error in malloc");
    }
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str, q), 0)
    free(q->predicates);
    q->predicates=NULL;
    q->projections=malloc(2*sizeof(projection));
    if(q->projections==NULL)
    {
        perror("error in malloc");
    }
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str, q), 0);
    free(q->projections);
    q->projections=NULL;
    delete_query(q);
}

/**
 * Test 2 Wrong Str without | and then with illegal characters
 * The function must return !=0
 */
void testAnalyze_query2()
{
    char* query_str="1 2 1.2=0.1 1.3";
    query* q=create_query();
    if(q!=NULL)
    {
        CU_ASSERT_NOT_EQUAL(analyze_query(query_str, q), 0)
                char query_illegal_str[20];
        strncpy(query_illegal_str, "1 2 1.2=0.1 1.3", 19);
        for(unsigned int i=0; i<UCHAR_MAX; i++)
        {
            query_illegal_str[5]=(char) i;
            printf("%s\n", query_illegal_str);
            CU_ASSERT_NOT_EQUAL(analyze_query(query_illegal_str, q), 0)
        }
        delete_query(q);
    }
}

/**
 * Test 3 Wrong Str without 1)tables 2)predicates 3) projections
 * The function must return !=0
 */
void testAnalyze_query3()
{
    query* q1=create_query();
    query* q2=create_query();
    query* q3=create_query();
    if(q1==NULL||q2==NULL||q3==NULL)
    {
        return;
    }
    char query_str1[20];
    char query_str2[20];
    char query_str3[20];
    strncpy(query_str1, "|1.2=0.1|1.3", 19);
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str1, q1), 0);
    strncpy(query_str2, "1 5||1.3", 19);
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str2, q2), 0);
    strncpy(query_str3, "1 5|1.2=0.1|", 19);
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str3, q3), 0);
    delete_query(q1);
    delete_query(q2);
    delete_query(q3);
}

/**
 * Test 4 Wrong Str with missing/or not permitted characters
 * The function must return !=0
 */
void testAnalyze_query4()
{
    query* q1=create_query();
    query* q2=create_query();
    query* q3=create_query();
    query* q4=create_query();
    query* q5=create_query();
    query* q6=create_query();
    query* q7=create_query();
    query* q8=create_query();
    query* q9=create_query();
    query* q10=create_query();
    query* q11=create_query();
    query* q12=create_query();
    query* q13=create_query();
    query* q14=create_query();
    query* q15=create_query();
    if(q1==NULL||q2==NULL||q3==NULL||q4==NULL||q5==NULL||
            q6==NULL||q7==NULL||q8==NULL||q9==NULL||q10==NULL||
            q11==NULL||q12==NULL||q13==NULL||q14==NULL||q15==NULL)
    {
        return;
    }
    char query_str1[30];
    char query_str2[30];
    char query_str3[30];
    char query_str4[30];
    char query_str5[30];
    char query_str6[30];
    char query_str7[30];
    char query_str8[30];
    char query_str9[30];
    char query_str10[30];
    char query_str11[30];
    char query_str12[30];
    char query_str13[30];
    char query_str14[30];
    char query_str15[30];
    strncpy(query_str1, "1 3 2|1.2=0.1|1.3 1 1.2", 29);
    strncpy(query_str2, "1 3 2|1.2=0.1|1.3 1. 1.4", 29);
    strncpy(query_str3, "1 3 2|1.2=0.1|1.3 .2 1.6", 29);
    strncpy(query_str4, "1.1 3 & 2|1.2=0.1|1.3 1.6", 29);
    strncpy(query_str5, "1 3 2|1.2=0.1&|1.3 1.6", 29);
    strncpy(query_str6, "1 3 2|&1.2=0.1&|1.3 1.6", 29);
    strncpy(query_str7, "1 3 2|1.2=0.1&&2.1>402|1.3 1.6", 29);
    strncpy(query_str8, "1 3 2|1.2=0.1&2.1>402&|1.3 1.6", 29);
    strncpy(query_str9, "1 3 2|&1.2=0.1&2.1>402&|1.3 1.6", 29);
    strncpy(query_str10, "1 3 2|1.2=&0.1&2.1>402|1.3 1.6", 29);
    strncpy(query_str11, "1 3 2|1.2==0.1&2.1>402|1.3 1.6", 29);
    strncpy(query_str12, "1 3 2|1.2=<0.1&2.1>402|1.3 1.6", 29);
    strncpy(query_str13, "1 3 2|1.2=>0.1&2.1>402|1.3 1.6", 29);
    strncpy(query_str14, "1 3 2|1.2<<0.1&2.1>402|1.3 1.6", 29);
    strncpy(query_str15, "1 3 2|1.2>>0.1&2.1>402|1.3 1.6", 29);
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str1, q1), 0);
    delete_query(q1);
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str2, q2), 0);
    delete_query(q2);
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str3, q3), 0);
    delete_query(q3);
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str4, q4), 0);
    delete_query(q4);
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str5, q5), 0);
    delete_query(q5);
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str6, q6), 0);
    delete_query(q6);
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str7, q7), 0);
    delete_query(q7);
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str8, q8), 0);
    delete_query(q8);
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str9, q9), 0);
    delete_query(q9);
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str10, q10), 0);
    delete_query(q10);
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str11, q11), 0);
    delete_query(q11);
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str12, q12), 0);
    delete_query(q12);
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str13, q13), 0);
    delete_query(q13);
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str14, q14), 0);
    delete_query(q14);
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str15, q15), 0);
    delete_query(q15);
}

/**
 * Test 5 Correct Queries
 * The function must return 0
 */
void testAnalyze_query5()
{
    query* q1=create_query();
    query* q2=create_query();
    query* q3=create_query();
    if(q1==NULL||q2==NULL||q3==NULL)
    {
        return;
    }
    char query_str1[45];
    char query_str2[45];
    char query_str3[150];
    strncpy(query_str1, "1 2|1.2=0.1|1.3", 44);
    strncpy(query_str2, "1 3 2|1.2=0.1&2.1>10203|1.3 2.4", 44);
    strncpy(query_str3, "  1  3 2   5  9 |1.2=0.1&1.1<2341&0.2=76588877766&0.1=3.1&2.1<>64987523&3.1<=9837&2.2>=2134&4.2=3.1|1.3 2.1 4.1 3.2 1.6", 149);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str1, q1), 0);
    CU_ASSERT_EQUAL_FATAL(q1->number_of_tables, 2);
    CU_ASSERT_EQUAL_FATAL(q1->number_of_predicates, 1);
    CU_ASSERT_EQUAL_FATAL(q1->number_of_projections, 1);
    CU_ASSERT_NOT_EQUAL_FATAL(q1->table_ids, NULL);
    CU_ASSERT_NOT_EQUAL_FATAL(q1->predicates, NULL);
    CU_ASSERT_NOT_EQUAL_FATAL(q1->projections, NULL);
    CU_ASSERT_EQUAL_FATAL(q1->table_ids[0], 1);
    CU_ASSERT_EQUAL_FATAL(q1->table_ids[1], 2);
    CU_ASSERT_EQUAL_FATAL(q1->predicates[0].type, Join);
    CU_ASSERT_EQUAL_FATAL(((predicate_join*) (q1->predicates[0].p))->r.table_id, 1);
    CU_ASSERT_EQUAL_FATAL(((predicate_join*) (q1->predicates[0].p))->r.column_id, 2);
    CU_ASSERT_EQUAL_FATAL(((predicate_join*) (q1->predicates[0].p))->s.table_id, 0);
    CU_ASSERT_EQUAL_FATAL(((predicate_join*) (q1->predicates[0].p))->s.column_id, 1);
    CU_ASSERT_EQUAL_FATAL(q1->projections[0].column_to_project.table_id, 1);
    CU_ASSERT_EQUAL_FATAL(q1->projections[0].column_to_project.column_id, 3);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str2, q2), 0);
    CU_ASSERT_EQUAL_FATAL(q2->number_of_tables, 3);
    CU_ASSERT_EQUAL_FATAL(q2->number_of_predicates, 2);
    CU_ASSERT_EQUAL_FATAL(q2->number_of_projections, 2);
    CU_ASSERT_NOT_EQUAL_FATAL(q2->table_ids, NULL);
    CU_ASSERT_NOT_EQUAL_FATAL(q2->predicates, NULL);
    CU_ASSERT_NOT_EQUAL_FATAL(q2->projections, NULL);
    CU_ASSERT_EQUAL_FATAL(q2->table_ids[0], 1);
    CU_ASSERT_EQUAL_FATAL(q2->table_ids[1], 3);
    CU_ASSERT_EQUAL_FATAL(q2->table_ids[2], 2);
    CU_ASSERT_EQUAL_FATAL(q2->predicates[0].type, Join);
    CU_ASSERT_EQUAL_FATAL(((predicate_join*) (q2->predicates[0].p))->r.table_id, 1);
    CU_ASSERT_EQUAL_FATAL(((predicate_join*) (q2->predicates[0].p))->r.column_id, 2);
    CU_ASSERT_EQUAL_FATAL(((predicate_join*) (q2->predicates[0].p))->s.table_id, 0);
    CU_ASSERT_EQUAL_FATAL(((predicate_join*) (q2->predicates[0].p))->s.column_id, 1);
    CU_ASSERT_EQUAL_FATAL(q2->predicates[1].type, Filter);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) q2->predicates[1].p)->filter_type, More);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) (q2->predicates[1].p))->r.table_id, 2);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) (q2->predicates[1].p))->r.column_id, 1);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) (q2->predicates[1].p))->value, 10203);
    CU_ASSERT_EQUAL_FATAL(q2->projections[0].column_to_project.table_id, 1);
    CU_ASSERT_EQUAL_FATAL(q2->projections[0].column_to_project.column_id, 3);
    CU_ASSERT_EQUAL_FATAL(q2->projections[1].column_to_project.table_id, 2);
    CU_ASSERT_EQUAL_FATAL(q2->projections[1].column_to_project.column_id, 4);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str3, q3), 0);
    CU_ASSERT_EQUAL_FATAL(q3->number_of_tables, 5);
    CU_ASSERT_EQUAL_FATAL(q3->number_of_predicates, 8);
    CU_ASSERT_EQUAL_FATAL(q3->number_of_projections, 5);
    CU_ASSERT_NOT_EQUAL_FATAL(q3->table_ids, NULL);
    CU_ASSERT_NOT_EQUAL_FATAL(q3->predicates, NULL);
    CU_ASSERT_NOT_EQUAL_FATAL(q3->projections, NULL);
    CU_ASSERT_EQUAL_FATAL(q3->table_ids[0], 1);
    CU_ASSERT_EQUAL_FATAL(q3->table_ids[1], 3);
    CU_ASSERT_EQUAL_FATAL(q3->table_ids[2], 2);
    CU_ASSERT_EQUAL_FATAL(q3->table_ids[3], 5);
    CU_ASSERT_EQUAL_FATAL(q3->table_ids[4], 9);
    CU_ASSERT_EQUAL_FATAL(q3->predicates[0].type, Join);
    CU_ASSERT_EQUAL_FATAL(((predicate_join*) (q3->predicates[0].p))->r.table_id, 1);
    CU_ASSERT_EQUAL_FATAL(((predicate_join*) (q3->predicates[0].p))->r.column_id, 2);
    CU_ASSERT_EQUAL_FATAL(((predicate_join*) (q3->predicates[0].p))->s.table_id, 0);
    CU_ASSERT_EQUAL_FATAL(((predicate_join*) (q3->predicates[0].p))->s.column_id, 1);
    CU_ASSERT_EQUAL_FATAL(q3->predicates[1].type, Filter);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) q3->predicates[1].p)->filter_type, Less);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) (q3->predicates[1].p))->r.table_id, 1);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) (q3->predicates[1].p))->r.column_id, 1);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) (q3->predicates[1].p))->value, 2341);
    CU_ASSERT_EQUAL_FATAL(q3->predicates[2].type, Filter);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) q3->predicates[2].p)->filter_type, Equal);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) (q3->predicates[2].p))->r.table_id, 0);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) (q3->predicates[2].p))->r.column_id, 2);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) (q3->predicates[2].p))->value, 76588877766);
    CU_ASSERT_EQUAL_FATAL(q3->predicates[3].type, Join);
    CU_ASSERT_EQUAL_FATAL(((predicate_join*) (q3->predicates[3].p))->r.table_id, 0);
    CU_ASSERT_EQUAL_FATAL(((predicate_join*) (q3->predicates[3].p))->r.column_id, 1);
    CU_ASSERT_EQUAL_FATAL(((predicate_join*) (q3->predicates[3].p))->s.table_id, 3);
    CU_ASSERT_EQUAL_FATAL(((predicate_join*) (q3->predicates[3].p))->s.column_id, 1);
    CU_ASSERT_EQUAL_FATAL(q3->predicates[4].type, Filter);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) q3->predicates[4].p)->filter_type, Not_Equal);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) (q3->predicates[4].p))->r.table_id, 2);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) (q3->predicates[4].p))->r.column_id, 1);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) (q3->predicates[4].p))->value, 64987523);
    CU_ASSERT_EQUAL_FATAL(q3->predicates[5].type, Filter);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) q3->predicates[5].p)->filter_type, Less_Equal);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) (q3->predicates[5].p))->r.table_id, 3);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) (q3->predicates[5].p))->r.column_id, 1);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) (q3->predicates[5].p))->value, 9837);
    CU_ASSERT_EQUAL_FATAL(q3->predicates[6].type, Filter);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) q3->predicates[6].p)->filter_type, More_Equal);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) (q3->predicates[6].p))->r.table_id, 2);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) (q3->predicates[6].p))->r.column_id, 2);
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) (q3->predicates[6].p))->value, 2134);
    CU_ASSERT_EQUAL_FATAL(q3->predicates[7].type, Join);
    CU_ASSERT_EQUAL_FATAL(((predicate_join*) (q3->predicates[7].p))->r.table_id, 4);
    CU_ASSERT_EQUAL_FATAL(((predicate_join*) (q3->predicates[7].p))->r.column_id, 2);
    CU_ASSERT_EQUAL_FATAL(((predicate_join*) (q3->predicates[7].p))->s.table_id, 3);
    CU_ASSERT_EQUAL_FATAL(((predicate_join*) (q3->predicates[7].p))->s.column_id, 1);
    CU_ASSERT_EQUAL_FATAL(q3->projections[0].column_to_project.table_id, 1);
    CU_ASSERT_EQUAL_FATAL(q3->projections[0].column_to_project.column_id, 3);
    CU_ASSERT_EQUAL_FATAL(q3->projections[1].column_to_project.table_id, 2);
    CU_ASSERT_EQUAL_FATAL(q3->projections[1].column_to_project.column_id, 1);
    CU_ASSERT_EQUAL_FATAL(q3->projections[2].column_to_project.table_id, 4);
    CU_ASSERT_EQUAL_FATAL(q3->projections[2].column_to_project.column_id, 1);
    CU_ASSERT_EQUAL_FATAL(q3->projections[3].column_to_project.table_id, 3);
    CU_ASSERT_EQUAL_FATAL(q3->projections[3].column_to_project.column_id, 2);
    CU_ASSERT_EQUAL_FATAL(q3->projections[4].column_to_project.table_id, 1);
    CU_ASSERT_EQUAL_FATAL(q3->projections[4].column_to_project.column_id, 6);
    delete_query(q1);delete_query(q2);delete_query(q3);
}

int main()
{
    CU_pSuite pSuite=NULL;

    /* Initialize the CUnit test registry */
    if(CUE_SUCCESS!=CU_initialize_registry())
        return CU_get_error();

    /* Add a suite to the registry */
    pSuite=CU_add_suite("newcunittest", init_suite, clean_suite);
    if(NULL==pSuite)
    {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* Add the tests to the suite */
    if((NULL==CU_add_test(pSuite, "testRemove_extra_chars", testRemove_extra_chars)||
            NULL==CU_add_test(pSuite, "testAnalyze_query1", testAnalyze_query1)||
            NULL==CU_add_test(pSuite, "testAnalyze_query2", testAnalyze_query2)||
            NULL==CU_add_test(pSuite, "testAnalyze_query3", testAnalyze_query3)||
            NULL==CU_add_test(pSuite, "testAnalyze_query4", testAnalyze_query4)||
            NULL==CU_add_test(pSuite, "testAnalyze_query5", testAnalyze_query5)
            ))
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
