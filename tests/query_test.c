#include <stdio.h>
#include <stdlib.h>
#include <CUnit/Basic.h>
#include "../query.h"
#include "../table.h"
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

bool compare_queries(query* q1, query*q2)
{
    if(q1==NULL||q2==NULL||q1->number_of_tables==0||q2->number_of_tables==0||
            q1->number_of_predicates==0||q2->number_of_predicates==0||
            q1->number_of_projections==0||q2->number_of_projections==0||
            q1->table_ids==NULL||q2->table_ids==NULL||q1->predicates==NULL||
            q2->predicates==NULL||q1->projections==NULL||q2->projections==NULL)
    {
        printf("Not Valid Queries\n");
        return false;
    }
    //Compare array sizes
    if(q1->number_of_tables!=q2->number_of_tables||
            q1->number_of_predicates!=q2->number_of_predicates||
            q1->number_of_projections!=q2->number_of_projections)
    {
        return false;
    }
    //Compare table ids
    for(uint32_t i=0; i<q1->number_of_tables; i++)
    {
        if(q1->table_ids[i]!=q2->table_ids[i])
        {
            return false;
        }
    }
    //Compare the predicates
    for(uint32_t i=0; i<q1->number_of_predicates; i++)
    {
        if(q1->predicates[i].type!=q2->predicates[i].type)
        {
            return false;
        }
        if(q1->predicates[i].type==Join||q1->predicates[i].type==Self_Join)
        {
            if(((predicate_join*) (q1->predicates[i].p))->r.table_id!=((predicate_join*) (q2->predicates[i].p))->r.table_id||
                    ((predicate_join*) (q1->predicates[i].p))->r.column_id!=((predicate_join*) (q2->predicates[i].p))->r.column_id||
                    ((predicate_join*) (q1->predicates[i].p))->s.table_id!=((predicate_join*) (q2->predicates[i].p))->s.table_id||
                    ((predicate_join*) (q1->predicates[i].p))->s.column_id!=((predicate_join*) (q2->predicates[i].p))->s.column_id)
            {
                return false;
            }
        }
        else
        {
            if(((predicate_filter*) (q1->predicates[i].p))->r.table_id!=((predicate_filter*) (q2->predicates[i].p))->r.table_id||
                    ((predicate_filter*) (q1->predicates[i].p))->r.column_id!=((predicate_filter*) (q2->predicates[i].p))->r.column_id||
                    ((predicate_filter*) (q1->predicates[i].p))->filter_type!=((predicate_filter*) (q2->predicates[i].p))->filter_type||
                    ((predicate_filter*) (q1->predicates[i].p))->value!=((predicate_filter*) (q2->predicates[i].p))->value)
            {
                return false;
            }
        }
    }
    //Compare the projections
    for(uint32_t i=0; i<q1->number_of_projections; i++)
    {
        if(q1->projections[i].column_to_project.table_id!=q2->projections[i].column_to_project.table_id||
                q1->projections[i].column_to_project.column_id!=q2->projections[i].column_to_project.column_id)
        {
            return false;
        }
    }
    return true;
}

void testRemove_extra_chars(void)
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
            char query_str10[80];
    strncpy(query_str10, "__ 12323 11_______ 455 _ 121 __ 12222 ____ ", 80);
    remove_extra_chars(query_str10, '_');
    CU_ASSERT_EQUAL(strcmp(query_str10, " 12323 11_ 455 _ 121 _ 12222 _ "), 0)
}

/**
 * Test 1 Null char*, Null query* and then not empty queries
 * The function must return !=0
 */
void testAnalyze_query1(void)
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
    q->number_of_tables=1;
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str, q), 0);
    q->number_of_tables=0;
    q->number_of_predicates=1;
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str, q), 0);
    q->number_of_predicates=0;
    q->number_of_projections=1;
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str, q), 0);
    q->number_of_projections=0;
    q->table_ids=malloc(2*sizeof(uint32_t));
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
void testAnalyze_query2(void)
{
    char* query_str="1 2 1.2=0.1 1.3";
    query* q=create_query();
    if(q!=NULL)
    {
        CU_ASSERT_NOT_EQUAL(analyze_query(query_str, q), 0)
                char query_illegal_str[20];
        strncpy(query_illegal_str, "1 2 1.2=0.1 1.3", 19);
        for(uint32_t i=0; i<UCHAR_MAX; i++)
        {
            query_illegal_str[5]=(char) i;
            //            printf("%s\n", query_illegal_str);
            CU_ASSERT_NOT_EQUAL(analyze_query(query_illegal_str, q), 0)
        }
        delete_query(q);
    }
}

/**
 * Test 3 Wrong Str without 1)tables 2)predicates 3) projections
 * The function must return !=0
 */
void testAnalyze_query3(void)
{
    query* q1=create_query();
    query* q2=create_query();
    query* q3=create_query();
    query* q4=create_query();
    if(q1==NULL||q2==NULL||q3==NULL||q4==NULL)
    {
        return;
    }
    char query_str1[20];
    char query_str2[20];
    char query_str3[20];
    char query_str4[20];
    strncpy(query_str1, "|1.2=0.1|1.3", 19);
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str1, q1), 0);
    strncpy(query_str2, "1 5||1.3", 19);
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str2, q2), 0);
    strncpy(query_str3, "1 5|1.2=0.1|", 19);
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str3, q3), 0);
    strncpy(query_str4, "||", 19);
    CU_ASSERT_NOT_EQUAL(analyze_query(query_str4, q4), 0);
    delete_query(q1);
    delete_query(q2);
    delete_query(q3);
    delete_query(q4);
}

/**
 * Test 4 Wrong Str with missing/or not permitted characters
 * The function must return !=0
 */
void testAnalyze_query4(void)
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
    char query_str1[50];
    char query_str2[50];
    char query_str3[50];
    char query_str4[50];
    char query_str5[50];
    char query_str6[50];
    char query_str7[50];
    char query_str8[50];
    char query_str9[50];
    char query_str10[50];
    char query_str11[50];
    char query_str12[50];
    char query_str13[50];
    char query_str14[50];
    char query_str15[50];
    strncpy(query_str1, "1 3 2|1.2=0.1|1.3 1 1.2", 49);
    strncpy(query_str2, "1 3 2|1.2=0.1|1.3 1. 1.4", 49);
    strncpy(query_str3, "1 3 2|1.2=0.1|1.3 .2 1.6", 49);
    strncpy(query_str4, "1.1 3 & 2|1.2=0.1|1.3 1.6", 49);
    strncpy(query_str5, "1 3 2|1.2=0.1&|1.3 1.6", 49);
    strncpy(query_str6, "1 3 2|&1.2=0.1&|1.3 1.6", 49);
    strncpy(query_str7, "1 3 2|1.2=0.1&&2.1>402|1.3 1.6", 49);
    strncpy(query_str8, "1 3 2|1.2=0.1&2.1>402&|1.3 1.6", 49);
    strncpy(query_str9, "1 3 2|&1.2=0.1&2.1>402&|1.3 1.6", 49);
    strncpy(query_str10, "1 3 2|1.2=&0.1&2.1>402|1.3 1.6", 49);
    strncpy(query_str11, "1 3 2|1.2==0.1&2.1>402|1.3 1.6", 49);
    strncpy(query_str12, "1 3 2|1.2=<0.1&2.1>402|1.3 1.6", 49);
    strncpy(query_str13, "1 3 2|1.2=>0.1&2.1>402|1.3 1.6", 49);
    strncpy(query_str14, "1 3 2|1.2<<0.1&2.1>402|1.3 1.6", 49);
    strncpy(query_str15, "1 3 2|1.2>>0.1&2.1>402|1.3 1.6", 49);
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
void testAnalyze_query5(void)
{
    query* q1=create_query();
    query* q2=create_query();
    query* q3=create_query();
    if(q1==NULL||q2==NULL||q3==NULL)
    {
        return;
    }
    char query_str1[80];
    char query_str2[80];
    char query_str3[150];
    strncpy(query_str1, "1 2|1.2=0.1|1.3", 79);
    strncpy(query_str2, "1 3 2|1.2=0.1&2.1>10203|1.3 2.4", 79);
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
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) q2->predicates[1].p)->filter_type, Greater);
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
    CU_ASSERT_EQUAL_FATAL(((predicate_filter*) q3->predicates[6].p)->filter_type, Greater_Equal);
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
    delete_query(q1);
    delete_query(q2);
    delete_query(q3);
}

/**
 * Test 1
 * The function must return 0
 */
void testValidate_query1(void)
{
    CU_ASSERT_EQUAL_FATAL(validate_query(NULL, NULL), -1); //NULL parameters
    query* q=create_query();
    CU_ASSERT_EQUAL_FATAL(validate_query(q, NULL), -1);
    table_index ti;
    ti.num_tables=0;
    CU_ASSERT_EQUAL_FATAL(validate_query(q, &ti), -1);
    ti.num_tables=1;
    CU_ASSERT_EQUAL_FATAL(validate_query(q, &ti), -1);
    ti.tables=malloc(sizeof(table));
    if(ti.tables==NULL)
    {
        perror("error in malloc");
        delete_query(q);
        return;
    }
    CU_ASSERT_EQUAL_FATAL(validate_query(q, &ti), -1);
    ti.tables[0].array=NULL;
    ti.tables[0].table_id=1;
    ti.tables[0].columns=3;
    ti.tables[0].rows=4;
    q->number_of_tables=1;
    CU_ASSERT_EQUAL_FATAL(validate_query(q, &ti), -1);
    q->number_of_predicates=1;
    CU_ASSERT_EQUAL_FATAL(validate_query(q, &ti), -1);
    q->number_of_projections=1;
    CU_ASSERT_EQUAL_FATAL(validate_query(q, &ti), -1);
    q->table_ids=malloc(sizeof(uint32_t));
    if(q->table_ids==NULL)
    {
        perror("error in malloc");
        return;
    }
    q->table_ids[0]=0;
    CU_ASSERT_EQUAL_FATAL(validate_query(q, &ti), -1);
    q->predicates=malloc(sizeof(predicate));
    if(q->predicates==NULL)
    {
        perror("error in malloc");
        return;
    }
    q->predicates[0].type=Filter;
    q->predicates[0].p=NULL;
    CU_ASSERT_EQUAL_FATAL(validate_query(q, &ti), -1);
    q->projections=malloc(sizeof(projection));
    if(q->projections==NULL)
    {
        perror("error in malloc");
        return;
    }
    q->projections[0].column_to_project.table_id=0;
    q->projections[0].column_to_project.column_id=0;
    CU_ASSERT_EQUAL_FATAL(validate_query(q, &ti), -4);
    delete_query(q);
    free(ti.tables);
}

void testValidate_query2(void)
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
    query* q16=create_query();
    query* q17=create_query();
    query* q18=create_query();
    query* q19=create_query();
    query* q20=create_query();
    query* q_c_1=create_query();
    query* q_c_17=create_query();
    query* q_c_18=create_query();
    query* q_c_19=create_query();
    query* q_c_20=create_query();
    if(q1==NULL||q2==NULL||q3==NULL||q4==NULL||q5==NULL||q6==NULL||q7==NULL
            ||q8==NULL||q9==NULL||q10==NULL||q11==NULL||q12==NULL
            ||q13==NULL||q14==NULL||q15==NULL||q_c_1==NULL
            ||q16==NULL||q17==NULL||q18==NULL||q_c_17==NULL||q_c_20==NULL
            ||q19==NULL||q20==NULL||q_c_18==NULL||q_c_19==NULL)
    {
        return;
    }
    table_index ti;
    ti.num_tables=4;
    ti.tables=malloc(sizeof(table)*4);
    if(ti.tables==NULL)
    {
        perror("testValidate_query2: malloc error");
        return;
    }
    ti.tables[0].array=NULL;
    ti.tables[0].table_id=0;
    ti.tables[0].columns=2;
    ti.tables[0].rows=0;
    ti.tables[1].array=NULL;
    ti.tables[1].table_id=1;
    ti.tables[1].columns=4;
    ti.tables[1].rows=0;
    ti.tables[2].array=NULL;
    ti.tables[2].table_id=2;
    ti.tables[2].columns=3;
    ti.tables[2].rows=0;
    ti.tables[3].array=NULL;
    ti.tables[3].table_id=3;
    ti.tables[3].columns=5;
    ti.tables[3].rows=0;
    char query_str1[80];
    char query_str2[80];
    char query_str3[80];
    char query_str4[80];
    char query_str5[80];
    char query_str6[80];
    char query_str7[80];
    char query_str8[80];
    char query_str9[80];
    char query_str10[80];
    char query_str11[80];
    char query_str12[80];
    char query_str13[80];
    char query_str14[80];
    char query_str15[100];
    char query_str_c_1[80];
    char query_str16[80];
    char query_str17[80];
    char query_str18[100];
    char query_str_c_17[80];
    char query_str_c_18[80];
    char query_str_c_19[80];
    char query_str19[100];
    char query_str20[200];
    char query_str_c_20[120];
    strncpy(query_str1, "0 1 2 3 4|1.2=2.0|1.1", 79); //Wrong table index
    strncpy(query_str2, "1 3 2|1.2=0.1&2.1=4.0|1.0 0.3", 79); //Wrong predicate s table index
    strncpy(query_str3, "1 3 2|1.2=0.1&4.1=1.0|1.0 0.3", 79); //Wrong predicate r table index
    strncpy(query_str4, "0 1 2|0.1=1.2&1.2=2.1|1.3 4.1", 79); //Wrong projection table index
    strncpy(query_str5, "0 1 2|1.5=0.1&2.1=0.1|1.3", 79); //Wrong predicate r column count
    strncpy(query_str6, "0 1 2|1.1=0.3&2.1=0.1|1.3", 79); //Wrong predicate s column count
    strncpy(query_str7, "0 1 2|0.1=1.2&1.2=2.1|1.3 2.10", 79); //Wrong projection column count
    strncpy(query_str8, "1 3 2|1.2=0.1&4.1>1125|1.0 0.3", 79); //Wrong predicate filter r table index
    strncpy(query_str9, "1 3 2|1.2=0.1&1.0=2.0&1.5>1125|1.0 0.3", 79); //Wrong predicate filter r column index
    strncpy(query_str10, "0 1 2|0.1=1.1&1.1=2.0&0.1=0.0&1.0>1125|1.0 0.1", 79); //Correct
    strncpy(query_str11, "0 1 2|0.1=1.1&1.1=2.0&0.1=0.1&1.0>1125|1.0 0.1", 79); //Correct
    //Check unnecessary tables removal
    strncpy(query_str12, "0 1 2 3|0.1=1.1&1.1=2.0&0.1=0.1&1.0=1125|1.0 0.1", 79); //Correct
    strncpy(query_str13, "0 1 3 2|0.1=1.1&1.1=3.0&0.1=0.1&1.0=1125|1.0 0.1", 79); //Correct
    strncpy(query_str14, "0 3 1 2|0.1=2.1&2.1=3.0&0.1=0.1&2.0=1125|2.0 0.1", 79); //Correct
    strncpy(query_str15, "3 0 1 2|1.1=2.1&2.1=3.0&1.1=1.1&2.0=1125|2.0 1.1", 99); //Correct
    strncpy(query_str_c_1, "0 1 2|0.1=1.1&1.1=2.0&1.0=1125|1.0 0.1", 79); //For verification
    //Check duplicate tables removal
    strncpy(query_str16, "0 1 2 2|0.1=1.1&1.1=2.0&0.1=0.1&1.0=1125|1.0 0.1", 79); //Correct Same as c_1
    strncpy(query_str17, "0 2 1 2|0.1=1.1&1.1=3.0&0.1=0.1&1.0>1125|1.0 0.1", 79); //Correct
    strncpy(query_str_c_17, "0 2 2|0.1=1.1&1.1=2.0&1.0>1125|1.0 0.1", 79); //For verification
    strncpy(query_str18, "2 0 1 2|0.1=2.1&2.1=3.0&0.1=0.1&3.0>1125|2.0 0.1 3.1", 79); //Correct
    strncpy(query_str_c_18, "2 0 1 2|0.1=2.1&2.1=3.0&3.0>1125|2.0 0.1 3.1", 79); //For verification

    strncpy(query_str19, "0 2 1 2 2 3|5.0=4.1&0.1=3.1&0.1=4.1&3.0=4.0&5.1>1000|4.1 5.1 0.1", 99); //Correct Same as c_1
    strncpy(query_str_c_19, "0 2 2 2 3|4.0=3.1&0.1=2.1&0.1=3.1&2.0=3.0&4.1>1000|3.1 4.1 0.1", 79); //Correct Same as c_1
    strncpy(query_str20, "3 1 0 2 0 0 2 1 2 3 1 2 3|10.1=11.0&6.0=7.1&7.0=10.1&3.1>1245&11.1>1245&6.1=1.1&4.1>1000&8.0=5.1&3.1=11.1&7.0=4.0|5.1 7.1 11.0", 199); //Correct
    strncpy(query_str_c_20, "1 0 2 0 0 2 1 2 1 2 |8.1=9.0&5.0=6.1&6.0=8.1&2.1>1245&9.1>1245&5.1=0.1&3.1>1000&7.0=4.1&2.1=9.1&6.0=3.0|4.1 6.1 9.0", 119); //For verification
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str1, q1), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str2, q2), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str3, q3), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str4, q4), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str5, q5), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str6, q6), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str7, q7), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str8, q8), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str9, q9), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str10, q10), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str11, q11), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str12, q12), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str13, q13), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str14, q14), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str15, q15), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str16, q16), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str17, q17), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str18, q18), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str19, q19), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str20, q20), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_1, q_c_1), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_17, q_c_17), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_18, q_c_18), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_19, q_c_19), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_20, q_c_20), 0);
    CU_ASSERT_NOT_EQUAL_FATAL(validate_query(q1, &ti), 0);
    CU_ASSERT_NOT_EQUAL_FATAL(validate_query(q2, &ti), 0);
    CU_ASSERT_NOT_EQUAL_FATAL(validate_query(q3, &ti), 0);
    CU_ASSERT_NOT_EQUAL_FATAL(validate_query(q4, &ti), 0);
    CU_ASSERT_NOT_EQUAL_FATAL(validate_query(q5, &ti), 0);
    CU_ASSERT_NOT_EQUAL_FATAL(validate_query(q6, &ti), 0);
    CU_ASSERT_NOT_EQUAL_FATAL(validate_query(q7, &ti), 0);
    CU_ASSERT_NOT_EQUAL_FATAL(validate_query(q8, &ti), 0);
    CU_ASSERT_NOT_EQUAL_FATAL(validate_query(q9, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q10, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(q10->number_of_predicates, 4);
    CU_ASSERT_EQUAL_FATAL(q10->predicates[2].type, Self_Join);
    CU_ASSERT_EQUAL_FATAL(validate_query(q11, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(q11->number_of_predicates, 3);
    CU_ASSERT_EQUAL_FATAL(validate_query(q12, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q13, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q14, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q15, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q16, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q17, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_1, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_17, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q18, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_18, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q19, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q20, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_19, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_20, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q12, q_c_1), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q13, q_c_1), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q14, q_c_1), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q15, q_c_1), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q16, q_c_1), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q17, q_c_17), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q18, q_c_18), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q19, q_c_19), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q20, q_c_20), true);
    delete_query(q1);
    delete_query(q2);
    delete_query(q3);
    delete_query(q4);
    delete_query(q5);
    delete_query(q6);
    delete_query(q7);
    delete_query(q8);
    delete_query(q9);
    delete_query(q10);
    delete_query(q11);
    delete_query(q12);
    delete_query(q13);
    delete_query(q14);
    delete_query(q15);
    delete_query(q16);
    delete_query(q17);
    delete_query(q18);
    delete_query(q19);
    delete_query(q20);
    delete_query(q_c_1);
    delete_query(q_c_17);
    delete_query(q_c_18);
    delete_query(q_c_19);
    delete_query(q_c_20);
    free(ti.tables);
}

bool compare_bool_arrays(bool* ar1, bool*ar2, size_t size)
{
    if(ar1==NULL||ar2==NULL||size==0)
    {
        return false;
    }
    for(size_t i=0; i<size; i++)
    {
        if(ar1[i]!=ar2[i])
        {
            return false;
        }
    }
    return true;
}

void testOptimize_query1(void)
{
    query* q1=create_query();
    query* q_c_1=create_query();
    query* q2=create_query();
    query* q_c_2=create_query();
    query* q3=create_query();
    query* q_c_3=create_query();
    query* q4=create_query();
    query* q_c_4=create_query();
    query* q5=create_query();
    query* q_c_5=create_query();
    query* q6=create_query();
    query* q_c_6=create_query();
    query* q7=create_query();
    query* q_c_7=create_query();
    query* q8=create_query();
    query* q_c_8=create_query();
    query* q9=create_query();
    query* q_c_9=create_query();
    query* q10=create_query();
    query* q_c_10=create_query();
    query* q11=create_query();
    query* q_c_11=create_query();
    query* q12=create_query();
    query* q_c_12=create_query();
    query* q13=create_query();
    query* q_c_13=create_query();
    query* q14=create_query();
    query* q_c_14=create_query();
    query* q15=create_query();
    query* q_c_15=create_query();
    query* q16=create_query();
    query* q_c_16=create_query();
    query* q17=create_query();
    query* q_c_17=create_query();
    query* q18=create_query();
    query* q_c_18=create_query();
    query* q19=create_query();
    query* q_c_19=create_query();
    query* q20=create_query();
    query* q_c_20=create_query();
    query* q21=create_query();
    query* q_c_21=create_query();
    query* q22=create_query();
    query* q_c_22=create_query();
    query* q23=create_query();
    query* q_c_23=create_query();
    query* q24=create_query();
    query* q_c_24=create_query();
    query* q25=create_query();
    query* q_c_25=create_query();
    query* q26=create_query();
    query* q_c_26=create_query();
    query* q27=create_query();
    query* q_c_27=create_query();
    query* q28=create_query();
    query* q_c_28=create_query();
    query* q29=create_query();
    query* q_c_29=create_query();
    query* q30=create_query();
    query* q_c_30=create_query();
    query* q31=create_query();
    query* q_c_31=create_query();
    query* q32=create_query();
    query* q_c_32=create_query();
    query* q33=create_query();
    query* q_c_33=create_query();
    query* q34=create_query();
    query* q_c_34=create_query();
    query* q35=create_query();
    query* q_c_35=create_query();
    query* q36=create_query();
    query* q_c_36=create_query();
    query* q37=create_query();
    query* q_c_37=create_query();
    query* q38=create_query();
    query* q_c_38=create_query();
    query* q39=create_query();
    query* q_c_39=create_query();
    query* q40=create_query();
    query* q_c_40=create_query();
    if(q1==NULL||q2==NULL||q3==NULL||q4==NULL||q5==NULL||q6==NULL||q7==NULL
            ||q8==NULL||q9==NULL||q10==NULL||q11==NULL||q12==NULL
            ||q13==NULL||q14==NULL||q15==NULL||q16==NULL||q17==NULL||q18==NULL||
            q19==NULL||q20==NULL||q21==NULL||
            q_c_1==NULL||q_c_2==NULL||q_c_3==NULL||q_c_4==NULL||q_c_5==NULL||
            q_c_6==NULL||q_c_7==NULL||q_c_8==NULL||q_c_9==NULL||q_c_10==NULL||
            q_c_11==NULL||q_c_12==NULL||q_c_13==NULL||q_c_14==NULL||q_c_15==NULL||
            q_c_16==NULL||q_c_17==NULL||q_c_18==NULL||q_c_19==NULL||q_c_20==NULL||
            q_c_21==NULL||q22==NULL||q23==NULL||q24==NULL||q25==NULL||q26==NULL||q27==NULL
            ||q28==NULL||q29==NULL||q30==NULL||q31==NULL||q32==NULL
            ||q33==NULL||q34==NULL||q35==NULL||q36==NULL||q37==NULL||q38==NULL||
            q39==NULL||q40==NULL||
            q_c_21==NULL||q_c_22==NULL||q_c_23==NULL||q_c_24==NULL||q_c_25==NULL||
            q_c_26==NULL||q_c_27==NULL||q_c_28==NULL||q_c_29==NULL||q_c_30==NULL||
            q_c_31==NULL||q_c_32==NULL||q_c_33==NULL||q_c_34==NULL||q_c_35==NULL||
            q_c_36==NULL||q_c_37==NULL||q_c_38==NULL||q_c_39==NULL||q_c_40==NULL
            )
    {
        return;
    }
    table_index ti;
    ti.num_tables=4;
    ti.tables=malloc(sizeof(table)*4);
    if(ti.tables==NULL)
    {
        perror("testOptimize_query1: malloc error");
        return;
    }
    ti.tables[0].array=NULL;
    ti.tables[0].table_id=0;
    ti.tables[0].columns=5;
    ti.tables[0].rows=467;
    ti.tables[1].array=NULL;
    ti.tables[1].table_id=1;
    ti.tables[1].columns=7;
    ti.tables[1].rows=7654;
    ti.tables[2].array=NULL;
    ti.tables[2].table_id=2;
    ti.tables[2].columns=9;
    ti.tables[2].rows=6876;
    ti.tables[3].array=NULL;
    ti.tables[3].table_id=3;
    ti.tables[3].columns=6;
    ti.tables[3].rows=124;
    char query_str1[80];
    char query_str_c_1[80];
    char query_str2[80];
    char query_str_c_2[80];
    char query_str3[80];
    char query_str_c_3[80];
    char query_str4[80];
    char query_str_c_4[80];
    char query_str5[80];
    char query_str_c_5[80];
    char query_str6[80];
    char query_str_c_6[80];
    char query_str7[80];
    char query_str_c_7[80];
    char query_str8[80];
    char query_str_c_8[80];
    char query_str9[80];
    char query_str_c_9[80];
    char query_str10[80];
    char query_str_c_10[80];
    char query_str11[80];
    char query_str_c_11[80];
    char query_str12[80];
    char query_str_c_12[80];
    char query_str13[80];
    char query_str_c_13[80];
    char query_str14[80];
    char query_str_c_14[80];
    char query_str15[100];
    char query_str_c_15[80];
    char query_str16[80];
    char query_str_c_16[80];
    char query_str17[80];
    char query_str_c_17[80];
    char query_str18[100];
    char query_str_c_18[80];
    char query_str19[100];
    char query_str_c_19[80];
    char query_str20[200];
    char query_str_c_20[120];
    char query_str21[200];
    char query_str_c_21[120];
    char query_str22[80];
    char query_str_c_22[80];
    char query_str23[80];
    char query_str_c_23[80];
    char query_str24[80];
    char query_str_c_24[80];
    char query_str25[80];
    char query_str_c_25[80];
    char query_str26[80];
    char query_str_c_26[80];
    char query_str27[80];
    char query_str_c_27[80];
    char query_str28[80];
    char query_str_c_28[80];
    char query_str29[80];
    char query_str_c_29[80];
    char query_str30[80];
    char query_str_c_30[80];
    char query_str31[80];
    char query_str_c_31[80];
    char query_str32[80];
    char query_str_c_32[80];
    char query_str33[80];
    char query_str_c_33[80];
    char query_str34[80];
    char query_str_c_34[80];
    char query_str35[100];
    char query_str_c_35[80];
    char query_str36[80];
    char query_str_c_36[80];
    char query_str37[80];
    char query_str_c_37[80];
    char query_str38[100];
    char query_str_c_38[80];
    char query_str39[100];
    char query_str_c_39[80];
    char query_str40[200];
    char query_str_c_40[120];
    strncpy(query_str1, "3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1", 79);
    strncpy(query_str_c_1, "3 0 1|0.2>3499&0.2=1.0&0.1=2.0|1.2 0.1", 79);
    bool q_b_1[]={true, true, true, true};
    strncpy(query_str2, "1 0|0.2=1.0&0.3=9881|1.1 0.2 1.0", 79);
    strncpy(query_str_c_2, "1 0|0.3=9881&0.2=1.0|1.1 0.2 1.0", 79);
    bool q_b_2[]={true, true};
    strncpy(query_str3, "3 0 2|0.1=1.0&1.0=2.2&0.0>12472|1.0 0.3 0.4", 79);
    strncpy(query_str_c_3, "3 0 2|0.0>12472&1.0=2.2&0.1=1.0|1.0 0.3 0.4", 79);
    bool q_b_3[]={true, true, true, false};
    strncpy(query_str4, "3 0|0.1=1.0&0.1>1150|0.3 1.0", 79);
    strncpy(query_str_c_4, "3 0|0.1>1150&0.1=1.0|0.3 1.0", 79);
    bool q_b_4[]={true, true};
    strncpy(query_str5, "2 1 3|0.1=1.0&1.0=2.2&0.0<62236|1.0", 79);
    strncpy(query_str_c_5, "2 1 3|0.0<62236&1.0=2.2&0.1=1.0|1.0", 79);
    bool q_b_5[]={true, true, true, false};
    strncpy(query_str6, "3 0 2|0.2=1.0&1.0=2.2&0.1=5784|2.3 0.1 0.1", 79);
    strncpy(query_str_c_6, "3 0 2|0.1=5784&1.0=2.2&0.2=1.0|2.3 0.1 0.1", 79);
    bool q_b_6[]={true, true, true, false};
    strncpy(query_str7, "0 1 2 3|0.1=1.0&1.0=2.1&1.0=3.1&0.1>2493|3.2 2.2 2.1", 79);
    strncpy(query_str_c_7, "0 1 2 3|0.1>2493&1.0=2.1&1.0=3.1&0.1=1.0|3.2 2.2 2.1", 79);
    bool q_b_7[]={true, true, false, true, true, false};
    strncpy(query_str8, "2 0 3 1|0.2=1.0&1.0=2.2&0.1=3.0&0.1=209|0.2 2.5 2.2", 79);
    strncpy(query_str_c_8, "2 0 3 1|0.1=209&1.0=2.2&0.2=1.0&0.1=3.0|0.2 2.5 2.2", 79);
    bool q_b_8[]={true, true, true, false, true, true};
    strncpy(query_str9, "0 1 2 3|0.1=1.0&1.0=2.1&1.0=3.1&0.0>44809|2.0", 79);
    strncpy(query_str_c_9, "0 1 2 3|0.0>44809&1.0=2.1&1.0=3.1&0.1=1.0|2.0", 79);
    bool q_b_9[]={true, true, false, true, true, false};
    strncpy(query_str10, "2 1 3|0.2=1.0&1.0=2.1&2.1=1.0&0.0>8107151&2.0<15412794|0.1 1.1", 79);
    strncpy(query_str_c_10, "2 1 3|2.0<15412794&0.0>8107151&1.0=0.2&1.0=2.1|0.1 1.1", 79);
    bool q_b_10[]={true, true, false, true};
    strncpy(query_str11, "2 1 0 2|0.2=1.0&1.0=2.1&0.1=3.2&3.0<33199|2.1 0.1 0.2", 79);
    strncpy(query_str_c_11, "2 1 0 2|3.0<33199&1.0=2.1&0.2=1.0&0.1=3.2|2.1 0.1 0.2", 79);
    bool q_b_11[]={true, true, true, false, true, true};
    strncpy(query_str12, "2 0 3 1|0.2=1.0&1.0=2.2&1.0=3.2&0.0<9872|3.3 2.2", 79);
    strncpy(query_str_c_12, "2 0 3 1|0.0<9872&1.0=2.2&1.0=3.2&0.2=1.0|3.3 2.2", 79);
    bool q_b_12[]={true, true, false, true, true, false};
    strncpy(query_str13, "2 0 1 1|0.2=1.0&1.0=2.2&2.1=3.2&0.1>7860|3.3 2.1 3.6", 79);
    strncpy(query_str_c_13, "2 0 1 1|0.1>7860&1.0=2.2&0.2=1.0&2.1=3.2|3.3 2.1 3.6", 79);
    bool q_b_13[]={true, true, true, false, true, true};
    strncpy(query_str14, "1 0 2 3|0.0=1.1&0.0=2.2&0.0=3.1&1.1>2936|1.0 1.0 3.0", 79);
    strncpy(query_str_c_14, "1 0 2 3|1.1>2936&0.0=2.2&0.0=3.1&0.0=1.1|1.0 1.0 3.0", 79);
    bool q_b_14[]={true, true, false, true, false, true};
    strncpy(query_str15, "1 0 2|0.1=1.0&1.0=0.1&1.0=2.1&0.1>3791|1.2 1.2", 79);
    strncpy(query_str_c_15, "1 0 2|0.1>3791&1.0=0.1&1.0=2.1|1.2 1.2", 79);
    bool q_b_15[]={true, true, false, true};
    strncpy(query_str16, "3 0 2|0.2=1.0&1.0=0.1&1.0=2.2&0.1>4477|2.0 2.3 1.2", 79);
    strncpy(query_str_c_16, "3 0 2|0.1>4477&1.0=0.1&1.0=2.2&0.2=1.0|2.0 2.3 1.2", 79);
    bool q_b_16[]={true, true, false, true};
    strncpy(query_str17, "1 0 2|0.0=1.2&0.0=2.1&1.1=0.0&1.0>25064|0.2 1.3", 79);
    strncpy(query_str_c_17, "1 0 2|1.0>25064&0.0=2.1&0.0=1.1&0.0=1.2|0.2 1.3", 79);
    bool q_b_17[]={true, true, false, true};
    strncpy(query_str18, "3 1 0|0.2=1.0&1.0=2.1&1.0=0.2&0.3>3991|1.0", 79);
    strncpy(query_str_c_18, "3 1 0|0.3>3991&1.0=2.1&0.2=1.0|1.0", 79);
    bool q_b_18[]={true, true, true, false};
    strncpy(query_str19, "3 0 2 2|0.2=1.0&1.0=2.1&2.1=3.2&0.2<74|1.2 2.5 3.5", 79);
    strncpy(query_str_c_19, "3 0 2 2|0.2<74&1.0=0.2&2.1=1.0&2.1=3.2|1.2 2.5 3.5", 79);
    bool q_b_19[]={true, true, true, false, false, true};
    strncpy(query_str20, "0 2 1 3|0.0=1.1&0.0=2.2&0.0=3.2&1.3=8728|2.0 3.1", 79);
    strncpy(query_str_c_20, "0 2 1 3|1.3=8728&0.0=2.2&0.0=3.2&0.0=1.1|2.0 3.1", 79);
    bool q_b_20[]={true, true, false, true, false, true};
    strncpy(query_str21, "0 3 2 1|0.0=1.2&0.0=2.1&0.0=3.2&1.2>295|3.2 0.0", 79);
    strncpy(query_str_c_21, "0 3 2 1|1.2>295&0.0=2.1&0.0=3.2&0.0=1.2|3.2 0.0", 79);
    bool q_b_21[]={true, true, false, true, false, true};
    strncpy(query_str22, "2 1 3|0.1=1.0&1.0=2.2&0.1=10731|1.2 2.3", 79);
    strncpy(query_str_c_22, "2 1 3|0.1=10731&1.0=2.2&0.1=1.0|1.2 2.3", 79);
    bool q_b_22[]={true, true, true, false};
    strncpy(query_str23, "3 1 0 2|0.1=1.0&1.0=2.1&1.0=3.2&0.2=4273|2.2 3.2", 79);
    strncpy(query_str_c_23, "3 1 0 2|0.2=4273&1.0=2.1&1.0=3.2&0.1=1.0|2.2 3.2", 79);
    bool q_b_23[]={true, true, false, true, true,false};
    strncpy(query_str24, "3 0 1|0.2=1.0&1.0=2.2&0.2>4041|1.0 1.1 1.0", 79);
    strncpy(query_str_c_24, "3 0 1|0.2>4041&1.0=2.2&0.2=1.0|1.0 1.1 1.0", 79);
    bool q_b_24[]={true, true, true, false};
    strncpy(query_str25, "0 1 3|0.1=1.0&1.0=2.1&0.0<13500|2.1 0.1 0.0", 79);
    strncpy(query_str_c_25, "0 1 3|0.0<13500&1.0=2.1&0.1=1.0|2.1 0.1 0.0", 79);
    bool q_b_25[]={true, true, true, false};
    strncpy(query_str26, "2 0 1|0.2=1.0&1.0=2.2&0.3<9473|0.3 2.0", 79);
    strncpy(query_str_c_26, "2 0 1|0.3<9473&1.0=2.2&0.2=1.0|0.3 2.0", 79);
    bool q_b_26[]={true, true, true, false};
    strncpy(query_str27, "0 1 3|0.2=1.0&1.0=2.1&1.0=0.2&0.2>6082|2.3 2.1", 79);
    strncpy(query_str_c_27, "0 1 3|0.2>6082&1.0=2.1&0.2=1.0|2.3 2.1", 79);
    bool q_b_27[]={true, true, true, false};
    strncpy(query_str28, "1 0 3|0.1=1.0&1.0=2.2&0.4=10571|2.3 0.0", 79);
    strncpy(query_str_c_28, "1 0 3|0.4=10571&1.0=2.2&0.1=1.0|2.3 0.0", 79);
    bool q_b_28[]={true, true, true, false};
    strncpy(query_str29, "3 1 2 0|0.1=1.0&1.0=2.1&1.0=3.1&0.2=598|3.2", 79);
    strncpy(query_str_c_29, "3 1 2 0|0.2=598&1.0=2.1&1.0=3.1&0.1=1.0|3.2", 79);
    bool q_b_29[]={true, true, false, true, true, false};
    strncpy(query_str30, "0 1 2|0.2=1.0&1.0=2.1&2.2=1.0&0.2<2685|2.0", 79);
    strncpy(query_str_c_30, "0 1 2|0.2<2685&1.0=2.1&1.0=2.2&0.2=1.0|2.0", 79);
    bool q_b_30[]={true, true, true, false};
    strncpy(query_str31, "2 0 1|0.2=1.0&1.0=2.1&0.3>10502|1.1 1.2 2.5", 79);
    strncpy(query_str_c_31, "2 0 1|0.3>10502&1.0=2.1&0.2=1.0|1.1 1.2 2.5", 79);
    bool q_b_31[]={true, true, true, false};
    strncpy(query_str32, "2 0 1|0.2=1.0&1.0=2.2&0.1<5283|0.0 0.2 2.3", 79);
    strncpy(query_str_c_32, "2 0 1|0.1<5283&1.0=2.2&0.2=1.0|0.0 0.2 2.3", 79);
    bool q_b_32[]={true, true, true, false};
    strncpy(query_str33, "2 1 3|0.1=1.0&1.0=2.2&0.1>345|0.0 1.2", 79);
    strncpy(query_str_c_33, "2 1 3|0.1>345&1.0=2.2&0.1=1.0|0.0 1.2", 79);
    bool q_b_33[]={true, true, true, false};
    strncpy(query_str34, "3 1 2|0.1=1.0&1.0=2.1&0.0>26374|2.0 0.1 2.1", 79);
    strncpy(query_str_c_34, "3 1 2|0.0>26374&1.0=2.1&0.1=1.0|2.0 0.1 2.1", 79);
    bool q_b_34[]={true, true, true, false};
    strncpy(query_str35, "1 0 2|0.2=1.0&1.0=2.2&0.1<4217|1.0", 79);
    strncpy(query_str_c_35, "1 0 2|0.1<4217&1.0=2.2&0.2=1.0|1.0", 79);
    bool q_b_35[]={true, true, true, false};
    strncpy(query_str36, "0 1 3|0.2=1.0&1.0=2.1&0.3<8722|1.0", 79);
    strncpy(query_str_c_36, "0 1 3|0.3<8722&1.0=2.1&0.2=1.0|1.0", 79);
    bool q_b_36[]={true, true, true, false};
    strncpy(query_str37, "2 1|0.1=1.0&0.1<9795|1.2 0.1", 79);
    strncpy(query_str_c_37, "2 1|0.1<9795&0.1=1.0|1.2 0.1", 79);
    bool q_b_37[]={true, true};
    strncpy(query_str38, "2 0 1|0.2=1.0&1.0=2.2&0.3=9477|0.2", 79);
    strncpy(query_str_c_38, "2 0 1|0.3=9477&1.0=2.2&0.2=1.0|0.2", 79);
    bool q_b_38[]={true, true, true, false};
    strncpy(query_str39, "0 1 2|0.1=1.0&1.0=2.1&0.1<3560|1.2", 79);
    strncpy(query_str_c_39, "0 1 2|0.1<3560&1.0=2.1&0.1=1.0|1.2", 79);
    bool q_b_39[]={true, true, true, false};
    strncpy(query_str40, "2 0 1 3|0.1=1.0&1.0=2.1&1.0=3.1&0.1=141152&2.2=10242743|0.1 2.2 2.1", 79);
    strncpy(query_str_c_40, "2 0 1 3|0.1=141152&2.2=10242743&1.0=3.1&1.0=0.1&1.0=2.1|0.1 2.2 2.1", 79);
    bool q_b_40[]={true, true, false, true, false, true};

    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str1, q1), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str2, q2), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str3, q3), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str4, q4), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str5, q5), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str6, q6), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str7, q7), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str8, q8), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str9, q9), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str10, q10), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str11, q11), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str12, q12), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str13, q13), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str14, q14), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str15, q15), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str16, q16), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str17, q17), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str18, q18), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str19, q19), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str20, q20), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str21, q21), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str22, q22), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str23, q23), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str24, q24), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str25, q25), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str26, q26), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str27, q27), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str28, q28), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str29, q29), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str30, q30), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str31, q31), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str32, q32), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str33, q33), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str34, q34), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str35, q35), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str36, q36), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str37, q37), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str38, q38), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str39, q39), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str40, q40), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q1, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q2, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q3, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q4, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q5, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q6, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q7, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q8, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q9, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q10, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q11, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q12, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q13, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q14, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q15, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q16, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q17, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q18, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q19, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q20, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q21, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q22, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q23, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q24, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q25, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q26, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q27, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q28, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q29, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q30, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q31, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q32, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q33, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q34, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q35, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q36, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q37, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q38, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q39, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q40, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q1, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q2, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q3, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q4, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q5, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q6, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q7, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q8, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q9, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q10, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q10, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q11, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q12, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q13, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q14, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q15, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q16, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q17, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q18, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q19, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q20, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q21, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q22, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q23, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q24, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q25, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q26, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q27, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q28, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q29, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q30, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q31, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q32, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q33, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q34, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q35, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q36, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q37, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q38, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q39, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q40, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_1, q_c_1), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_1, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q1, q_c_1), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_2, q_c_2), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_2, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q2, q_c_2), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_3, q_c_3), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_3, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q3, q_c_3), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_4, q_c_4), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_4, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q4, q_c_4), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_5, q_c_5), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_5, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q5, q_c_5), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_6, q_c_6), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_6, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q6, q_c_6), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_7, q_c_7), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_7, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q7, q_c_7), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_8, q_c_8), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_8, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q8, q_c_8), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_9, q_c_9), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_9, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q9, q_c_9), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_10, q_c_10), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_10, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q10, q_c_10), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_11, q_c_11), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_11, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q11, q_c_11), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_12, q_c_12), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_12, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q12, q_c_12), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_13, q_c_13), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_13, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q13, q_c_13), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_14, q_c_14), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_14, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q14, q_c_14), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_15, q_c_15), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_15, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q15, q_c_15), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_16, q_c_16), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_16, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q16, q_c_16), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_17, q_c_17), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_17, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q17, q_c_17), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_18, q_c_18), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_18, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q18, q_c_18), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_19, q_c_19), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_19, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q19, q_c_19), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_20, q_c_20), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_20, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q20, q_c_20), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_21, q_c_21), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_21, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q21, q_c_21), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_22, q_c_22), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_22, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q22, q_c_22), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_23, q_c_23), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_23, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q23, q_c_23), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_24, q_c_24), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_24, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q24, q_c_24), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_25, q_c_25), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_25, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q25, q_c_25), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_26, q_c_26), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_26, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q26, q_c_26), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_27, q_c_27), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_27, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q27, q_c_27), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_28, q_c_28), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_28, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q28, q_c_28), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_29, q_c_29), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_29, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q29, q_c_29), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_30, q_c_30), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_30, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q30, q_c_30), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_31, q_c_31), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_31, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q31, q_c_31), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_32, q_c_32), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_32, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q32, q_c_32), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_33, q_c_33), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_33, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q33, q_c_33), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_34, q_c_34), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_34, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q34, q_c_34), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_35, q_c_35), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_35, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q35, q_c_35), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_36, q_c_36), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_36, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q36, q_c_36), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_37, q_c_37), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_37, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q37, q_c_37), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_38, q_c_38), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_38, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q38, q_c_38), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_39, q_c_39), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_39, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q39, q_c_39), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_40, q_c_40), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_40, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q40, q_c_40), true);
    bool* array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q1, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_1, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q2, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_2, 2), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q3, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_3, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q4, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_4, 2), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q5, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_5, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q6, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_6, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q7, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_7, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q8, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_8, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q9, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_9, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q10, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_10, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q11, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_11, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q12, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_12, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q13, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_13, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q14, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_14, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q15, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_15, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q16, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_16, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q17, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_17, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q18, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_18, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q19, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_19, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q20, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_20, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q21, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_21, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q22, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_22, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q23, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_23, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q24, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_24, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q25, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_25, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q26, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_26, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q27, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_27, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q28, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_28, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q29, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_29, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q30, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_30, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q31, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_31, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q32, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_32, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q33, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_33, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q34, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_34, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q35, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_35, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q36, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_36, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q37, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_37, 2), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q38, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_38, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q39, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_39, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q40, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_40, 6), true);
    free(array);
    array=NULL;
    delete_query(q1);
    delete_query(q2);
    delete_query(q3);
    delete_query(q4);
    delete_query(q5);
    delete_query(q6);
    delete_query(q7);
    delete_query(q8);
    delete_query(q9);
    delete_query(q10);
    delete_query(q11);
    delete_query(q12);
    delete_query(q13);
    delete_query(q14);
    delete_query(q15);
    delete_query(q16);
    delete_query(q17);
    delete_query(q18);
    delete_query(q19);
    delete_query(q20);
    delete_query(q21);
    delete_query(q22);
    delete_query(q23);
    delete_query(q24);
    delete_query(q25);
    delete_query(q26);
    delete_query(q27);
    delete_query(q28);
    delete_query(q29);
    delete_query(q30);
    delete_query(q31);
    delete_query(q32);
    delete_query(q33);
    delete_query(q34);
    delete_query(q35);
    delete_query(q36);
    delete_query(q37);
    delete_query(q38);
    delete_query(q39);
    delete_query(q40);
    delete_query(q_c_1);
    delete_query(q_c_2);
    delete_query(q_c_3);
    delete_query(q_c_4);
    delete_query(q_c_5);
    delete_query(q_c_6);
    delete_query(q_c_7);
    delete_query(q_c_8);
    delete_query(q_c_9);
    delete_query(q_c_10);
    delete_query(q_c_11);
    delete_query(q_c_12);
    delete_query(q_c_13);
    delete_query(q_c_14);
    delete_query(q_c_15);
    delete_query(q_c_16);
    delete_query(q_c_17);
    delete_query(q_c_18);
    delete_query(q_c_19);
    delete_query(q_c_20);
    delete_query(q_c_21);
    delete_query(q_c_22);
    delete_query(q_c_23);
    delete_query(q_c_24);
    delete_query(q_c_25);
    delete_query(q_c_26);
    delete_query(q_c_27);
    delete_query(q_c_28);
    delete_query(q_c_29);
    delete_query(q_c_30);
    delete_query(q_c_31);
    delete_query(q_c_32);
    delete_query(q_c_33);
    delete_query(q_c_34);
    delete_query(q_c_35);
    delete_query(q_c_36);
    delete_query(q_c_37);
    delete_query(q_c_38);
    delete_query(q_c_39);
    delete_query(q_c_40);
    free(ti.tables);
}

void testOptimize_query_memory1(void)
{
    query* q1=create_query();
    query* q_c_1=create_query();
    query* q2=create_query();
    query* q_c_2=create_query();
    query* q3=create_query();
    query* q_c_3=create_query();
    query* q4=create_query();
    query* q_c_4=create_query();
    query* q5=create_query();
    query* q_c_5=create_query();
    query* q6=create_query();
    query* q_c_6=create_query();
    query* q7=create_query();
    query* q_c_7=create_query();
    query* q8=create_query();
    query* q_c_8=create_query();
    query* q9=create_query();
    query* q_c_9=create_query();
    query* q10=create_query();
    query* q_c_10=create_query();
    query* q11=create_query();
    query* q_c_11=create_query();
    query* q12=create_query();
    query* q_c_12=create_query();
    query* q13=create_query();
    query* q_c_13=create_query();
    query* q14=create_query();
    query* q_c_14=create_query();
    query* q15=create_query();
    query* q_c_15=create_query();
    query* q16=create_query();
    query* q_c_16=create_query();
    query* q17=create_query();
    query* q_c_17=create_query();
    query* q18=create_query();
    query* q_c_18=create_query();
    query* q19=create_query();
    query* q_c_19=create_query();
    query* q20=create_query();
    query* q_c_20=create_query();
    query* q21=create_query();
    query* q_c_21=create_query();
    query* q22=create_query();
    query* q_c_22=create_query();
    query* q23=create_query();
    query* q_c_23=create_query();
    query* q24=create_query();
    query* q_c_24=create_query();
    query* q25=create_query();
    query* q_c_25=create_query();
    query* q26=create_query();
    query* q_c_26=create_query();
    query* q27=create_query();
    query* q_c_27=create_query();
    query* q28=create_query();
    query* q_c_28=create_query();
    query* q29=create_query();
    query* q_c_29=create_query();
    query* q30=create_query();
    query* q_c_30=create_query();
    query* q31=create_query();
    query* q_c_31=create_query();
    query* q32=create_query();
    query* q_c_32=create_query();
    query* q33=create_query();
    query* q_c_33=create_query();
    query* q34=create_query();
    query* q_c_34=create_query();
    query* q35=create_query();
    query* q_c_35=create_query();
    query* q36=create_query();
    query* q_c_36=create_query();
    query* q37=create_query();
    query* q_c_37=create_query();
    query* q38=create_query();
    query* q_c_38=create_query();
    query* q39=create_query();
    query* q_c_39=create_query();
    query* q40=create_query();
    query* q_c_40=create_query();
    if(q1==NULL||q2==NULL||q3==NULL||q4==NULL||q5==NULL||q6==NULL||q7==NULL
            ||q8==NULL||q9==NULL||q10==NULL||q11==NULL||q12==NULL
            ||q13==NULL||q14==NULL||q15==NULL||q16==NULL||q17==NULL||q18==NULL||
            q19==NULL||q20==NULL||q21==NULL||
            q_c_1==NULL||q_c_2==NULL||q_c_3==NULL||q_c_4==NULL||q_c_5==NULL||
            q_c_6==NULL||q_c_7==NULL||q_c_8==NULL||q_c_9==NULL||q_c_10==NULL||
            q_c_11==NULL||q_c_12==NULL||q_c_13==NULL||q_c_14==NULL||q_c_15==NULL||
            q_c_16==NULL||q_c_17==NULL||q_c_18==NULL||q_c_19==NULL||q_c_20==NULL||
            q_c_21==NULL||q22==NULL||q23==NULL||q24==NULL||q25==NULL||q26==NULL||q27==NULL
            ||q28==NULL||q29==NULL||q30==NULL||q31==NULL||q32==NULL
            ||q33==NULL||q34==NULL||q35==NULL||q36==NULL||q37==NULL||q38==NULL||
            q39==NULL||q40==NULL||
            q_c_21==NULL||q_c_22==NULL||q_c_23==NULL||q_c_24==NULL||q_c_25==NULL||
            q_c_26==NULL||q_c_27==NULL||q_c_28==NULL||q_c_29==NULL||q_c_30==NULL||
            q_c_31==NULL||q_c_32==NULL||q_c_33==NULL||q_c_34==NULL||q_c_35==NULL||
            q_c_36==NULL||q_c_37==NULL||q_c_38==NULL||q_c_39==NULL||q_c_40==NULL
            )
    {
        return;
    }
    table_index ti;
    ti.num_tables=4;
    ti.tables=malloc(sizeof(table)*4);
    if(ti.tables==NULL)
    {
        perror("testOptimize_query1: malloc error");
        return;
    }
    ti.tables[0].array=NULL;
    ti.tables[0].table_id=0;
    ti.tables[0].columns=5;
    ti.tables[0].rows=467;
    ti.tables[1].array=NULL;
    ti.tables[1].table_id=1;
    ti.tables[1].columns=7;
    ti.tables[1].rows=7654;
    ti.tables[2].array=NULL;
    ti.tables[2].table_id=2;
    ti.tables[2].columns=9;
    ti.tables[2].rows=6876;
    ti.tables[3].array=NULL;
    ti.tables[3].table_id=3;
    ti.tables[3].columns=6;
    ti.tables[3].rows=124;
    char query_str1[80];
    char query_str_c_1[80];
    char query_str2[80];
    char query_str_c_2[80];
    char query_str3[80];
    char query_str_c_3[80];
    char query_str4[80];
    char query_str_c_4[80];
    char query_str5[80];
    char query_str_c_5[80];
    char query_str6[80];
    char query_str_c_6[80];
    char query_str7[80];
    char query_str_c_7[80];
    char query_str8[80];
    char query_str_c_8[80];
    char query_str9[80];
    char query_str_c_9[80];
    char query_str10[80];
    char query_str_c_10[80];
    char query_str11[80];
    char query_str_c_11[80];
    char query_str12[80];
    char query_str_c_12[80];
    char query_str13[80];
    char query_str_c_13[80];
    char query_str14[80];
    char query_str_c_14[80];
    char query_str15[100];
    char query_str_c_15[80];
    char query_str16[80];
    char query_str_c_16[80];
    char query_str17[80];
    char query_str_c_17[80];
    char query_str18[100];
    char query_str_c_18[80];
    char query_str19[100];
    char query_str_c_19[80];
    char query_str20[200];
    char query_str_c_20[120];
    char query_str21[200];
    char query_str_c_21[120];
    char query_str22[80];
    char query_str_c_22[80];
    char query_str23[80];
    char query_str_c_23[80];
    char query_str24[80];
    char query_str_c_24[80];
    char query_str25[80];
    char query_str_c_25[80];
    char query_str26[80];
    char query_str_c_26[80];
    char query_str27[80];
    char query_str_c_27[80];
    char query_str28[80];
    char query_str_c_28[80];
    char query_str29[80];
    char query_str_c_29[80];
    char query_str30[80];
    char query_str_c_30[80];
    char query_str31[80];
    char query_str_c_31[80];
    char query_str32[80];
    char query_str_c_32[80];
    char query_str33[80];
    char query_str_c_33[80];
    char query_str34[80];
    char query_str_c_34[80];
    char query_str35[100];
    char query_str_c_35[80];
    char query_str36[80];
    char query_str_c_36[80];
    char query_str37[80];
    char query_str_c_37[80];
    char query_str38[100];
    char query_str_c_38[80];
    char query_str39[100];
    char query_str_c_39[80];
    char query_str40[200];
    char query_str_c_40[120];
    strncpy(query_str1, "3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1", 79);
    strncpy(query_str_c_1, "3 0 1|0.2>3499&0.2=1.0&0.1=2.0|1.2 0.1", 79);
    bool q_b_1[]={true, true, true, true};
    strncpy(query_str2, "1 0|0.2=1.0&0.3=9881|1.1 0.2 1.0", 79);
    strncpy(query_str_c_2, "1 0|0.3=9881&0.2=1.0|1.1 0.2 1.0", 79);
    bool q_b_2[]={true, true};
    strncpy(query_str3, "3 0 2|0.1=1.0&1.0=2.2&0.0>12472|1.0 0.3 0.4", 79);
    strncpy(query_str_c_3, "3 0 2|1.0=2.2&0.0>12472&0.1=1.0|1.0 0.3 0.4", 79);
    bool q_b_3[]={true, true, true, false};
    strncpy(query_str4, "3 0|0.1=1.0&0.1>1150|0.3 1.0", 79);
    strncpy(query_str_c_4, "3 0|0.1>1150&0.1=1.0|0.3 1.0", 79);
    bool q_b_4[]={true, true};
    strncpy(query_str5, "2 1 3|0.1=1.0&1.0=2.2&0.0<62236|1.0", 79);
    strncpy(query_str_c_5, "2 1 3|1.0=2.2&0.0<62236&0.1=1.0|1.0", 79);
    bool q_b_5[]={true, true, true, false};
    strncpy(query_str6, "3 0 2|0.2=1.0&1.0=2.2&0.1=5784|2.3 0.1 0.1", 79);
    strncpy(query_str_c_6, "3 0 2|1.0=2.2&0.1=5784&0.2=1.0|2.3 0.1 0.1", 79);
    bool q_b_6[]={true, true, true, false};
    strncpy(query_str7, "0 1 2 3|0.1=1.0&1.0=2.1&1.0=3.1&0.1>2493|3.2 2.2 2.1", 79);
    strncpy(query_str_c_7, "0 1 2 3|1.0=2.1&1.0=3.1&0.1>2493&0.1=1.0|3.2 2.2 2.1", 79);
    bool q_b_7[]={true, true, false, true, true, false};
    strncpy(query_str8, "2 0 3 1|0.2=1.0&1.0=2.2&0.1=3.0&0.1=209|0.2 2.5 2.2", 79);
    strncpy(query_str_c_8, "2 0 3 1|1.0=2.2&0.1=209&0.2=1.0&0.1=3.0|0.2 2.5 2.2", 79);
    bool q_b_8[]={true, true, true, false, true, true};
    strncpy(query_str9, "0 1 2 3|0.1=1.0&1.0=2.1&1.0=3.1&0.0>44809|2.0", 79);
    strncpy(query_str_c_9, "0 1 2 3|1.0=2.1&1.0=3.1&0.0>44809&0.1=1.0|2.0", 79);
    bool q_b_9[]={true, true, false, true, true, false};
    strncpy(query_str10, "2 1 3|0.2=1.0&1.0=2.1&2.1=1.0&0.0>8107151&2.0<15412794|0.1 1.1", 79);
    strncpy(query_str_c_10, "2 1 3|0.0>8107151&1.0=0.2&2.0<15412794&1.0=2.1|0.1 1.1", 79);
    bool q_b_10[]={true, true, false, true};
    strncpy(query_str11, "2 1 0 2|0.2=1.0&1.0=2.1&0.1=3.2&3.0<33199|2.1 0.1 0.2", 79);
    strncpy(query_str_c_11, "2 1 0 2|1.0=2.1&0.2=1.0&3.0<33199&0.1=3.2|2.1 0.1 0.2", 79);
    bool q_b_11[]={true, true, true, false, true, true};
    strncpy(query_str12, "2 0 3 1|0.2=1.0&1.0=2.2&1.0=3.2&0.0<9872|3.3 2.2", 79);
    strncpy(query_str_c_12, "2 0 3 1|1.0=2.2&1.0=3.2&0.0<9872&0.2=1.0|3.3 2.2", 79);
    bool q_b_12[]={true, true, false, true, true, false};
    strncpy(query_str13, "2 0 1 1|0.2=1.0&1.0=2.2&2.1=3.2&0.1>7860|3.3 2.1 3.6", 79);
    strncpy(query_str_c_13, "2 0 1 1|1.0=2.2&0.1>7860&0.2=1.0&2.1=3.2|3.3 2.1 3.6", 79);
    bool q_b_13[]={true, true, true, false, true, true};
    strncpy(query_str14, "1 0 2 3|0.0=1.1&0.0=2.2&0.0=3.1&1.1>2936|1.0 1.0 3.0", 79);
    strncpy(query_str_c_14, "1 0 2 3|0.0=2.2&0.0=3.1&1.1>2936&0.0=1.1|1.0 1.0 3.0", 79);
    bool q_b_14[]={true, true, false, true, false, true};
    strncpy(query_str15, "1 0 2|0.1=1.0&1.0=0.1&1.0=2.1&0.1>3791|1.2 1.2", 79);
    strncpy(query_str_c_15, "1 0 2|0.1>3791&1.0=0.1&1.0=2.1|1.2 1.2", 79);
    bool q_b_15[]={true, true, false, true};
    strncpy(query_str16, "3 0 2|0.2=1.0&1.0=0.1&1.0=2.2&0.1>4477|2.0 2.3 1.2", 79);
    strncpy(query_str_c_16, "3 0 2|0.1>4477&1.0=0.1&1.0=2.2&0.2=1.0|2.0 2.3 1.2", 79);
    bool q_b_16[]={true, true, false, true};
    strncpy(query_str17, "1 0 2|0.0=1.2&0.0=2.1&1.1=0.0&1.0>25064|0.2 1.3", 79);
    strncpy(query_str_c_17, "1 0 2|0.0=2.1&1.0>25064&0.0=1.1&0.0=1.2|0.2 1.3", 79);
    bool q_b_17[]={true, true, false, true};
    strncpy(query_str18, "3 1 0|0.2=1.0&1.0=2.1&1.0=0.2&0.3>3991|1.0", 79);
    strncpy(query_str_c_18, "3 1 0|1.0=2.1&0.3>3991&0.2=1.0|1.0", 79);
    bool q_b_18[]={true, true, true, false};
    strncpy(query_str19, "3 0 2 2|0.2=1.0&1.0=2.1&2.1=3.2&0.2<74|1.2 2.5 3.5", 79);
    strncpy(query_str_c_19, "3 0 2 2|0.2<74&1.0=0.2&2.1=1.0&2.1=3.2|1.2 2.5 3.5", 79);
    bool q_b_19[]={true, true, true, false, false, true};
    strncpy(query_str20, "0 2 1 3|0.0=1.1&0.0=2.2&0.0=3.2&1.3=8728|2.0 3.1", 79);
    strncpy(query_str_c_20, "0 2 1 3|0.0=2.2&0.0=3.2&1.3=8728&0.0=1.1|2.0 3.1", 79);
    bool q_b_20[]={true, true, false, true, false, true};
    strncpy(query_str21, "0 3 2 1|0.0=1.2&0.0=2.1&0.0=3.2&1.2>295|3.2 0.0", 79);
    strncpy(query_str_c_21, "0 3 2 1|0.0=2.1&0.0=3.2&1.2>295&0.0=1.2|3.2 0.0", 79);
    bool q_b_21[]={true, true, false, true, false, true};
    strncpy(query_str22, "2 1 3|0.1=1.0&1.0=2.2&0.1=10731|1.2 2.3", 79);
    strncpy(query_str_c_22, "2 1 3|1.0=2.2&0.1=10731&0.1=1.0|1.2 2.3", 79);
    bool q_b_22[]={true, true, true, false};
    strncpy(query_str23, "3 1 0 2|0.1=1.0&1.0=2.1&1.0=3.2&0.2=4273|2.2 3.2", 79);
    strncpy(query_str_c_23, "3 1 0 2|1.0=2.1&1.0=3.2&0.2=4273&0.1=1.0|2.2 3.2", 79);
    bool q_b_23[]={true, true, false, true, true,false};
    strncpy(query_str24, "3 0 1|0.2=1.0&1.0=2.2&0.2>4041|1.0 1.1 1.0", 79);
    strncpy(query_str_c_24, "3 0 1|1.0=2.2&0.2>4041&0.2=1.0|1.0 1.1 1.0", 79);
    bool q_b_24[]={true, true, true, false};
    strncpy(query_str25, "0 1 3|0.1=1.0&1.0=2.1&0.0<13500|2.1 0.1 0.0", 79);
    strncpy(query_str_c_25, "0 1 3|1.0=2.1&0.0<13500&0.1=1.0|2.1 0.1 0.0", 79);
    bool q_b_25[]={true, true, true, false};
    strncpy(query_str26, "2 0 1|0.2=1.0&1.0=2.2&0.3<9473|0.3 2.0", 79);
    strncpy(query_str_c_26, "2 0 1|1.0=2.2&0.3<9473&0.2=1.0|0.3 2.0", 79);
    bool q_b_26[]={true, true, true, false};
    strncpy(query_str27, "0 1 3|0.2=1.0&1.0=2.1&1.0=0.2&0.2>6082|2.3 2.1", 79);
    strncpy(query_str_c_27, "0 1 3|1.0=2.1&0.2>6082&0.2=1.0|2.3 2.1", 79);
    bool q_b_27[]={true, true, true, false};
    strncpy(query_str28, "1 0 3|0.1=1.0&1.0=2.2&0.4=10571|2.3 0.0", 79);
    strncpy(query_str_c_28, "1 0 3|1.0=2.2&0.4=10571&0.1=1.0|2.3 0.0", 79);
    bool q_b_28[]={true, true, true, false};
    strncpy(query_str29, "3 1 2 0|0.1=1.0&1.0=2.1&1.0=3.1&0.2=598|3.2", 79);
    strncpy(query_str_c_29, "3 1 2 0|1.0=2.1&1.0=3.1&0.2=598&0.1=1.0|3.2", 79);
    bool q_b_29[]={true, true, false, true, true, false};
    strncpy(query_str30, "0 1 2|0.2=1.0&1.0=2.1&2.2=1.0&0.2<2685|2.0", 79);
    strncpy(query_str_c_30, "0 1 2|1.0=2.1&1.0=2.2&0.2<2685&0.2=1.0|2.0", 79);
    bool q_b_30[]={true, true, true, false};
    strncpy(query_str31, "2 0 1|0.2=1.0&1.0=2.1&0.3>10502|1.1 1.2 2.5", 79);
    strncpy(query_str_c_31, "2 0 1|1.0=2.1&0.3>10502&0.2=1.0|1.1 1.2 2.5", 79);
    bool q_b_31[]={true, true, true, false};
    strncpy(query_str32, "2 0 1|0.2=1.0&1.0=2.2&0.1<5283|0.0 0.2 2.3", 79);
    strncpy(query_str_c_32, "2 0 1|1.0=2.2&0.1<5283&0.2=1.0|0.0 0.2 2.3", 79);
    bool q_b_32[]={true, true, true, false};
    strncpy(query_str33, "2 1 3|0.1=1.0&1.0=2.2&0.1>345|0.0 1.2", 79);
    strncpy(query_str_c_33, "2 1 3|1.0=2.2&0.1>345&0.1=1.0|0.0 1.2", 79);
    bool q_b_33[]={true, true, true, false};
    strncpy(query_str34, "3 1 2|0.1=1.0&1.0=2.1&0.0>26374|2.0 0.1 2.1", 79);
    strncpy(query_str_c_34, "3 1 2|1.0=2.1&0.0>26374&0.1=1.0|2.0 0.1 2.1", 79);
    bool q_b_34[]={true, true, true, false};
    strncpy(query_str35, "1 0 2|0.2=1.0&1.0=2.2&0.1<4217|1.0", 79);
    strncpy(query_str_c_35, "1 0 2|1.0=2.2&0.1<4217&0.2=1.0|1.0", 79);
    bool q_b_35[]={true, true, true, false};
    strncpy(query_str36, "0 1 3|0.2=1.0&1.0=2.1&0.3<8722|1.0", 79);
    strncpy(query_str_c_36, "0 1 3|1.0=2.1&0.3<8722&0.2=1.0|1.0", 79);
    bool q_b_36[]={true, true, true, false};
    strncpy(query_str37, "2 1|0.1=1.0&0.1<9795|1.2 0.1", 79);
    strncpy(query_str_c_37, "2 1|0.1<9795&0.1=1.0|1.2 0.1", 79);
    bool q_b_37[]={true, true};
    strncpy(query_str38, "2 0 1|0.2=1.0&1.0=2.2&0.3=9477|0.2", 79);
    strncpy(query_str_c_38, "2 0 1|1.0=2.2&0.3=9477&0.2=1.0|0.2", 79);
    bool q_b_38[]={true, true, true, false};
    strncpy(query_str39, "0 1 2|0.1=1.0&1.0=2.1&0.1<3560|1.2", 79);
    strncpy(query_str_c_39, "0 1 2|1.0=2.1&0.1<3560&0.1=1.0|1.2", 79);
    bool q_b_39[]={true, true, true, false};
    strncpy(query_str40, "2 0 1 3|0.1=1.0&1.0=2.1&1.0=3.1&0.1=141152&2.2=10242743|0.1 2.2 2.1", 79);
    strncpy(query_str_c_40, "2 0 1 3|1.0=3.1&0.1=141152&1.0=0.1&2.2=10242743&1.0=2.1|0.1 2.2 2.1", 79);
    bool q_b_40[]={true, true, false, true, false, true};

    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str1, q1), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str2, q2), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str3, q3), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str4, q4), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str5, q5), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str6, q6), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str7, q7), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str8, q8), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str9, q9), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str10, q10), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str11, q11), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str12, q12), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str13, q13), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str14, q14), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str15, q15), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str16, q16), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str17, q17), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str18, q18), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str19, q19), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str20, q20), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str21, q21), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str22, q22), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str23, q23), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str24, q24), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str25, q25), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str26, q26), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str27, q27), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str28, q28), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str29, q29), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str30, q30), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str31, q31), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str32, q32), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str33, q33), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str34, q34), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str35, q35), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str36, q36), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str37, q37), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str38, q38), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str39, q39), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str40, q40), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q1, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q2, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q3, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q4, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q5, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q6, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q7, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q8, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q9, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q10, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q11, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q12, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q13, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q14, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q15, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q16, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q17, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q18, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q19, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q20, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q21, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q22, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q23, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q24, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q25, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q26, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q27, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q28, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q29, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q30, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q31, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q32, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q33, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q34, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q35, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q36, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q37, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q38, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q39, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q40, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q1, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q2, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q3, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q4, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q5, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q6, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q7, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q8, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q9, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q10, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q10, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q11, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q12, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q13, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q14, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q15, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q16, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q17, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q18, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q19, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q20, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q21, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q22, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q23, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q24, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q25, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q26, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q27, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q28, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q29, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q30, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q31, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q32, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q33, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q34, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q35, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q36, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q37, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q38, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q39, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q40, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_1, q_c_1), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_1, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_2, q_c_2), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_2, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_3, q_c_3), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_3, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_4, q_c_4), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_4, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_5, q_c_5), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_5, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_6, q_c_6), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_6, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_7, q_c_7), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_7, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_8, q_c_8), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_8, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_9, q_c_9), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_9, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_10, q_c_10), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_10, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_11, q_c_11), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_11, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_12, q_c_12), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_12, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_13, q_c_13), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_13, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_14, q_c_14), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_14, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_15, q_c_15), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_15, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_16, q_c_16), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_16, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_17, q_c_17), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_17, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_18, q_c_18), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_18, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_19, q_c_19), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_19, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_20, q_c_20), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_20, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_21, q_c_21), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_21, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_22, q_c_22), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_22, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_23, q_c_23), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_23, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_24, q_c_24), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_24, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_25, q_c_25), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_25, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_26, q_c_26), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_26, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_27, q_c_27), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_27, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_28, q_c_28), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_28, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_29, q_c_29), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_29, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_30, q_c_30), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_30, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_31, q_c_31), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_31, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_32, q_c_32), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_32, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_33, q_c_33), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_33, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_34, q_c_34), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_34, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_35, q_c_35), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_35, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_36, q_c_36), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_36, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_37, q_c_37), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_37, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_38, q_c_38), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_38, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_39, q_c_39), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_39, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_40, q_c_40), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_40, &ti), 0);
    bool* array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q1, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_1, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q2, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_2, 2), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q3, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_3, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q4, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_4, 2), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q5, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_5, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q6, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_6, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q7, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_7, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q8, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_8, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q9, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_9, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q10, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_10, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q11, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_11, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q12, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_12, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q13, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_13, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q14, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_14, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q15, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_15, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q16, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_16, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q17, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_17, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q18, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_18, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q19, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_19, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q20, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_20, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q21, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_21, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q22, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_22, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q23, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_23, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q24, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_24, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q25, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_25, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q26, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_26, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q27, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_27, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q28, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_28, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q29, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_29, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q30, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_30, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q31, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_31, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q32, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_32, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q33, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_33, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q34, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_34, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q35, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_35, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q36, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_36, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q37, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_37, 2), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q38, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_38, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q39, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_39, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q40, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_40, 6), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q1), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q2), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q3), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q4), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q5), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q6), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q7), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q8), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q9), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q10), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q11), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q12), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q13), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q14), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q15), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q16), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q17), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q18), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q19), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q20), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q21), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q22), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q23), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q24), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q25), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q26), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q27), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q28), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q29), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q30), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q31), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q32), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q33), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q34), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q35), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q36), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q37), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q38), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q39), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q40), 0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q1, q_c_1), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q2, q_c_2), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q3, q_c_3), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q4, q_c_4), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q5, q_c_5), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q6, q_c_6), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q7, q_c_7), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q8, q_c_8), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q9, q_c_9), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q10, q_c_10), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q11, q_c_11), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q12, q_c_12), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q13, q_c_13), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q14, q_c_14), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q15, q_c_15), true);
    q_c_16->predicates[3].type=Self_Join;
    CU_ASSERT_EQUAL_FATAL(compare_queries(q16, q_c_16), true);
    q_c_17->predicates[3].type=Self_Join;
    CU_ASSERT_EQUAL_FATAL(compare_queries(q17, q_c_17), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q18, q_c_18), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q19, q_c_19), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q20, q_c_20), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q21, q_c_21), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q22, q_c_22), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q23, q_c_23), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q24, q_c_24), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q25, q_c_25), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q26, q_c_26), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q27, q_c_27), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q28, q_c_28), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q29, q_c_29), true);
    q_c_30->predicates[1].type=Self_Join;
    CU_ASSERT_EQUAL_FATAL(compare_queries(q30, q_c_30), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q31, q_c_31), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q32, q_c_32), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q33, q_c_33), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q34, q_c_34), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q35, q_c_35), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q36, q_c_36), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q37, q_c_37), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q38, q_c_38), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q39, q_c_39), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q40, q_c_40), true);

    delete_query(q1);
    delete_query(q2);
    delete_query(q3);
    delete_query(q4);
    delete_query(q5);
    delete_query(q6);
    delete_query(q7);
    delete_query(q8);
    delete_query(q9);
    delete_query(q10);
    delete_query(q11);
    delete_query(q12);
    delete_query(q13);
    delete_query(q14);
    delete_query(q15);
    delete_query(q16);
    delete_query(q17);
    delete_query(q18);
    delete_query(q19);
    delete_query(q20);
    delete_query(q21);
    delete_query(q22);
    delete_query(q23);
    delete_query(q24);
    delete_query(q25);
    delete_query(q26);
    delete_query(q27);
    delete_query(q28);
    delete_query(q29);
    delete_query(q30);
    delete_query(q31);
    delete_query(q32);
    delete_query(q33);
    delete_query(q34);
    delete_query(q35);
    delete_query(q36);
    delete_query(q37);
    delete_query(q38);
    delete_query(q39);
    delete_query(q40);
    delete_query(q_c_1);
    delete_query(q_c_2);
    delete_query(q_c_3);
    delete_query(q_c_4);
    delete_query(q_c_5);
    delete_query(q_c_6);
    delete_query(q_c_7);
    delete_query(q_c_8);
    delete_query(q_c_9);
    delete_query(q_c_10);
    delete_query(q_c_11);
    delete_query(q_c_12);
    delete_query(q_c_13);
    delete_query(q_c_14);
    delete_query(q_c_15);
    delete_query(q_c_16);
    delete_query(q_c_17);
    delete_query(q_c_18);
    delete_query(q_c_19);
    delete_query(q_c_20);
    delete_query(q_c_21);
    delete_query(q_c_22);
    delete_query(q_c_23);
    delete_query(q_c_24);
    delete_query(q_c_25);
    delete_query(q_c_26);
    delete_query(q_c_27);
    delete_query(q_c_28);
    delete_query(q_c_29);
    delete_query(q_c_30);
    delete_query(q_c_31);
    delete_query(q_c_32);
    delete_query(q_c_33);
    delete_query(q_c_34);
    delete_query(q_c_35);
    delete_query(q_c_36);
    delete_query(q_c_37);
    delete_query(q_c_38);
    delete_query(q_c_39);
    delete_query(q_c_40);
    free(ti.tables);
    CU_ASSERT_EQUAL_FATAL(true, true);
}

int main(void)
{
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);
    CU_pSuite pSuite=NULL;

    /* Initialize the CUnit test registry */
    if(CUE_SUCCESS!=CU_initialize_registry())
    {
        return CU_get_error();
    }
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
            NULL==CU_add_test(pSuite, "testAnalyze_query5", testAnalyze_query5)||
            NULL==CU_add_test(pSuite, "testValidate_query1", testValidate_query1)||
            NULL==CU_add_test(pSuite, "testValidate_query2", testValidate_query2)||
            NULL==CU_add_test(pSuite, "testOptimize_query1", testOptimize_query1)||
            NULL==CU_add_test(pSuite, "testOptimize_query_memory1", testOptimize_query_memory1)
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
