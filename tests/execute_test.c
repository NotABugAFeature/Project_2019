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

uint64_t calculate_sum_test(query*,projection*, table_index*, middleman *);
uint64_t calculate_sum_test(query* q,projection*p, table_index* index, middleman *m)
{
    if(q==NULL||p==NULL||index==NULL||m==NULL)
    {
        return 0;
    }
    table* t= get_table(index, q->table_ids[p->column_to_project.table_id]);
    if(t == NULL)
    {
      return 0;
    }
    uint64_t sum = 0;
    middle_list_node *list_temp_node = m->tables[p->column_to_project.table_id].list->head;
    while(list_temp_node != NULL)
    {
      for(unsigned int i = 0; i < list_temp_node->bucket.index_to_add_next; i++)
      {
        sum += t->array[p->column_to_project.column_id][list_temp_node->bucket.row_ids[i]];
      }
      list_temp_node = list_temp_node->next;
    }
    return sum;
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
    query* q10=create_query();query* q11=create_query();
    if(q1==NULL||q2==NULL||q3==NULL||q4==NULL||q5==NULL||q6==NULL||q7==NULL||
       q8==NULL||q9==NULL||q10==NULL||q11==NULL)
    {
        delete_table_index(ti);
        CU_ASSERT(0);
        return;
    }
    char query_str1[200];char query_str2[200];char query_str3[200];
    char query_str4[200];char query_str5[200];char query_str6[200];
    char query_str7[200];char query_str8[200];char query_str9[200];
    char query_str10[200];char query_str11[200];
    strncpy(query_str1, "0|0.0<1000|0.0 0.1 0.2", 199);
    strncpy(query_str2, "0|0.2>500|0.0 0.1 0.2", 199);
    strncpy(query_str3, "0|0.1=8824|0.0 0.1 0.2", 199);
    strncpy(query_str4, "0|0.0<1000&0.2>1500|0.0 0.1 0.2", 199);
    strncpy(query_str5, "2|0.2=0.3|0.0 0.1 0.2 0.3", 199);
    strncpy(query_str6, "0 1|0.0=1.0|0.0 0.1 0.2 1.0 1.1 1.2", 199);
    strncpy(query_str7, "0 1|0.0=1.0&0.0=1.1|0.0 0.1 0.2 1.0 1.1 1.2", 199);
    strncpy(query_str8, "0 1 2 3 4|0.0=1.0&2.1=3.2&4.0=1.2&4.0=3.2|0.0 0.1 0.2 1.0 1.1 1.2 2.0 2.1 2.2 2.3 3.0 3.1 3.2 3.3 4.0 4.1 ", 199);
    strncpy(query_str9, "0 1 2 3 4|0.1=1.2&1.1=3.1&3.1=2.2&2.3=4.1&0.0>2000&3.2>1000&4.1=2379|0.0 0.1 0.2 1.0 1.1 1.2 2.0 2.1 2.2 2.3 3.0 3.1 3.2 3.3 4.0 4.1 ", 199);
    strncpy(query_str10, " ", 199);
    strncpy(query_str11, " ", 199);
    bool* bool_array=NULL;
    if(analyze_query(query_str1,q1)==0&&validate_query(q1, ti)==0&&
       optimize_query(q1, ti)==0&&create_sort_array(q1, &bool_array)==0&&
       optimize_query_memory(q1)==0
       )
    {
        middleman *middle=execute_query(q1, ti, bool_array);
        //Check the row count
        CU_ASSERT_EQUAL(middle_list_get_number_of_records(middle->tables[0].list),332);
        //Ckeck the sums
        CU_ASSERT_EQUAL(calculate_sum_test(q1,&q1->projections[0],ti,middle),165722);
        CU_ASSERT_EQUAL(calculate_sum_test(q1,&q1->projections[1],ti,middle),2420825);
        CU_ASSERT_EQUAL(calculate_sum_test(q1,&q1->projections[2],ti,middle),1385996);
        for(uint32_t i=0; i<middle->number_of_tables; i++)
        {
            if(middle->tables[i].list!=NULL)
            {
                delete_middle_list(middle->tables[i].list);
            }
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
    if(analyze_query(query_str2,q2)==0&&validate_query(q2, ti)==0&&
       optimize_query(q2, ti)==0&&create_sort_array(q2, &bool_array)==0&&
       optimize_query_memory(q2)==0
       )
    {
        middleman *middle=execute_query(q2, ti, bool_array);
        //Check the row count
        CU_ASSERT_EQUAL(middle_list_get_number_of_records(middle->tables[0].list),1503);
        //Ckeck the sums
        CU_ASSERT_EQUAL(calculate_sum_test(q2,&q2->projections[0],ti,middle),3525316);
        CU_ASSERT_EQUAL(calculate_sum_test(q2,&q2->projections[1],ti,middle),10969255);
        CU_ASSERT_EQUAL(calculate_sum_test(q2,&q2->projections[2],ti,middle),6983208);
        for(uint32_t i=0; i<middle->number_of_tables; i++)
        {
            if(middle->tables[i].list!=NULL)
            {
                delete_middle_list(middle->tables[i].list);
            }
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
    if(analyze_query(query_str3,q3)==0&&validate_query(q3, ti)==0&&
       optimize_query(q3, ti)==0&&create_sort_array(q3, &bool_array)==0&&
       optimize_query_memory(q3)==0
       )
    {
        middleman *middle=execute_query(q3, ti, bool_array);
        //Check the row count
        CU_ASSERT_EQUAL(middle_list_get_number_of_records(middle->tables[0].list),2);
        //Ckeck the sums
        CU_ASSERT_EQUAL(calculate_sum_test(q3,&q3->projections[0],ti,middle),3536);
        CU_ASSERT_EQUAL(calculate_sum_test(q3,&q3->projections[1],ti,middle),17648);
        CU_ASSERT_EQUAL(calculate_sum_test(q3,&q3->projections[2],ti,middle),7776);
        for(uint32_t i=0; i<middle->number_of_tables; i++)
        {
            if(middle->tables[i].list!=NULL)
            {
                delete_middle_list(middle->tables[i].list);
            }
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
    if(analyze_query(query_str4,q4)==0&&validate_query(q4, ti)==0&&
       optimize_query(q4, ti)==0&&create_sort_array(q4, &bool_array)==0&&
       optimize_query_memory(q4)==0
       )
    {
        middleman *middle=execute_query(q4, ti, bool_array);
        //Check the row count
        CU_ASSERT_EQUAL(middle_list_get_number_of_records(middle->tables[0].list),275);
        //Ckeck the sums
        CU_ASSERT_EQUAL(calculate_sum_test(q4,&q4->projections[0],ti,middle),141959);
        CU_ASSERT_EQUAL(calculate_sum_test(q4,&q4->projections[1],ti,middle),1989017);
        CU_ASSERT_EQUAL(calculate_sum_test(q4,&q4->projections[2],ti,middle),1339457);
        for(uint32_t i=0; i<middle->number_of_tables; i++)
        {
            if(middle->tables[i].list!=NULL)
            {
                delete_middle_list(middle->tables[i].list);
            }
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
    if(analyze_query(query_str5,q5)==0&&validate_query(q5, ti)==0&&
       optimize_query(q5, ti)==0&&create_sort_array(q5, &bool_array)==0&&
       optimize_query_memory(q5)==0
       )
    {
        middleman *middle=execute_query(q5, ti, bool_array);
        //Check the row count
        CU_ASSERT_EQUAL(middle_list_get_number_of_records(middle->tables[0].list),8);
        //Ckeck the sums
        CU_ASSERT_EQUAL(calculate_sum_test(q5,&q5->projections[0],ti,middle),395727);
        CU_ASSERT_EQUAL(calculate_sum_test(q5,&q5->projections[1],ti,middle),35260);
        CU_ASSERT_EQUAL(calculate_sum_test(q5,&q5->projections[2],ti,middle),19814);
        CU_ASSERT_EQUAL(calculate_sum_test(q5,&q5->projections[3],ti,middle),19814);
        for(uint32_t i=0; i<middle->number_of_tables; i++)
        {
            if(middle->tables[i].list!=NULL)
            {
                delete_middle_list(middle->tables[i].list);
            }
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
    if(analyze_query(query_str6,q6)==0&&validate_query(q6, ti)==0&&
       optimize_query(q6, ti)==0&&create_sort_array(q6, &bool_array)==0&&
       optimize_query_memory(q6)==0
       )
    {
        middleman *middle=execute_query(q6, ti, bool_array);
        //Check the row count
        CU_ASSERT_EQUAL(middle_list_get_number_of_records(middle->tables[0].list),494);
        //Ckeck the sums
        CU_ASSERT_EQUAL(calculate_sum_test(q6,&q6->projections[0],ti,middle),1141020);
        CU_ASSERT_EQUAL(calculate_sum_test(q6,&q6->projections[1],ti,middle),3645882);
        CU_ASSERT_EQUAL(calculate_sum_test(q6,&q6->projections[2],ti,middle),2224263);
        CU_ASSERT_EQUAL(calculate_sum_test(q6,&q6->projections[3],ti,middle),1141020);
        CU_ASSERT_EQUAL(calculate_sum_test(q6,&q6->projections[4],ti,middle),2134690);
        CU_ASSERT_EQUAL(calculate_sum_test(q6,&q6->projections[5],ti,middle),3538934);
        for(uint32_t i=0; i<middle->number_of_tables; i++)
        {
            if(middle->tables[i].list!=NULL)
            {
                delete_middle_list(middle->tables[i].list);
            }
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
    if(analyze_query(query_str7,q7)==0&&validate_query(q7, ti)==0&&
       optimize_query(q7, ti)==0&&create_sort_array(q7, &bool_array)==0&&
       optimize_query_memory(q7)==0
       )
    {
        middleman *middle=execute_query(q7, ti, bool_array);
        //Check the row count
        CU_ASSERT_EQUAL(middle_list_get_number_of_records(middle->tables[0].list),1);
        //Ckeck the sums
        CU_ASSERT_EQUAL(calculate_sum_test(q7,&q7->projections[0],ti,middle),3083);
        CU_ASSERT_EQUAL(calculate_sum_test(q7,&q7->projections[1],ti,middle),5391);
        CU_ASSERT_EQUAL(calculate_sum_test(q7,&q7->projections[2],ti,middle),4191);
        CU_ASSERT_EQUAL(calculate_sum_test(q7,&q7->projections[3],ti,middle),3083);
        CU_ASSERT_EQUAL(calculate_sum_test(q7,&q7->projections[4],ti,middle),3083);
        CU_ASSERT_EQUAL(calculate_sum_test(q7,&q7->projections[5],ti,middle),4956);
        for(uint32_t i=0; i<middle->number_of_tables; i++)
        {
            if(middle->tables[i].list!=NULL)
            {
                delete_middle_list(middle->tables[i].list);
            }
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
    if(analyze_query(query_str8,q8)==0&&validate_query(q8, ti)==0&&
       optimize_query(q8, ti)==0&&create_sort_array(q8, &bool_array)==0&&
       optimize_query_memory(q8)==0
       )
    {
        middleman *middle=execute_query(q8, ti, bool_array);
        //Check the row count
        CU_ASSERT_EQUAL(middle_list_get_number_of_records(middle->tables[0].list),104);
        //Ckeck the sums
        CU_ASSERT_EQUAL(calculate_sum_test(q8,&q8->projections[0],ti,middle),301912);
        CU_ASSERT_EQUAL(calculate_sum_test(q8,&q8->projections[1],ti,middle),1026896);
        CU_ASSERT_EQUAL(calculate_sum_test(q8,&q8->projections[2],ti,middle),299728);
        CU_ASSERT_EQUAL(calculate_sum_test(q8,&q8->projections[3],ti,middle),301912);
        CU_ASSERT_EQUAL(calculate_sum_test(q8,&q8->projections[4],ti,middle),410280);
        CU_ASSERT_EQUAL(calculate_sum_test(q8,&q8->projections[5],ti,middle),453648);
        CU_ASSERT_EQUAL(calculate_sum_test(q8,&q8->projections[6],ti,middle),4610801);
        CU_ASSERT_EQUAL(calculate_sum_test(q8,&q8->projections[7],ti,middle),453648);
        CU_ASSERT_EQUAL(calculate_sum_test(q8,&q8->projections[8],ti,middle),290147);
        CU_ASSERT_EQUAL(calculate_sum_test(q8,&q8->projections[9],ti,middle),255957);
        CU_ASSERT_EQUAL(calculate_sum_test(q8,&q8->projections[10],ti,middle),3631848);
        CU_ASSERT_EQUAL(calculate_sum_test(q8,&q8->projections[11],ti,middle),530584);
        CU_ASSERT_EQUAL(calculate_sum_test(q8,&q8->projections[12],ti,middle),453648);
        CU_ASSERT_EQUAL(calculate_sum_test(q8,&q8->projections[13],ti,middle),580264);
        CU_ASSERT_EQUAL(calculate_sum_test(q8,&q8->projections[14],ti,middle),453648);
        CU_ASSERT_EQUAL(calculate_sum_test(q8,&q8->projections[15],ti,middle),560976);
        for(uint32_t i=0; i<middle->number_of_tables; i++)
        {
            if(middle->tables[i].list!=NULL)
            {
                delete_middle_list(middle->tables[i].list);
            }
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
    if(analyze_query(query_str9,q9)==0&&validate_query(q9, ti)==0&&
       optimize_query(q9, ti)==0&&create_sort_array(q9, &bool_array)==0&&
       optimize_query_memory(q9)==0
       )
    {
        middleman *middle=execute_query(q9, ti, bool_array);
        //Check the row count
        CU_ASSERT_EQUAL(middle_list_get_number_of_records(middle->tables[0].list),54);
        //Ckeck the sums
        CU_ASSERT_EQUAL(calculate_sum_test(q9,&q9->projections[0],ti,middle),202254);
        CU_ASSERT_EQUAL(calculate_sum_test(q9,&q9->projections[1],ti,middle),414696);
        CU_ASSERT_EQUAL(calculate_sum_test(q9,&q9->projections[2],ti,middle),328536);
        CU_ASSERT_EQUAL(calculate_sum_test(q9,&q9->projections[3],ti,middle),427086);
        CU_ASSERT_EQUAL(calculate_sum_test(q9,&q9->projections[4],ti,middle),237234);
        CU_ASSERT_EQUAL(calculate_sum_test(q9,&q9->projections[5],ti,middle),414696);
        CU_ASSERT_EQUAL(calculate_sum_test(q9,&q9->projections[6],ti,middle),1208532);
        CU_ASSERT_EQUAL(calculate_sum_test(q9,&q9->projections[7],ti,middle),525306);
        CU_ASSERT_EQUAL(calculate_sum_test(q9,&q9->projections[8],ti,middle),237234);
        CU_ASSERT_EQUAL(calculate_sum_test(q9,&q9->projections[9],ti,middle),128466);
        CU_ASSERT_EQUAL(calculate_sum_test(q9,&q9->projections[10],ti,middle),1916646);
        CU_ASSERT_EQUAL(calculate_sum_test(q9,&q9->projections[11],ti,middle),237234);
        CU_ASSERT_EQUAL(calculate_sum_test(q9,&q9->projections[12],ti,middle),146850);
        CU_ASSERT_EQUAL(calculate_sum_test(q9,&q9->projections[13],ti,middle),285090);
        CU_ASSERT_EQUAL(calculate_sum_test(q9,&q9->projections[14],ti,middle),272142);
        CU_ASSERT_EQUAL(calculate_sum_test(q9,&q9->projections[15],ti,middle),128466);
        for(uint32_t i=0; i<middle->number_of_tables; i++)
        {
            if(middle->tables[i].list!=NULL)
            {
                delete_middle_list(middle->tables[i].list);
            }
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
    delete_query(q9);delete_query(q10);delete_query(q11);
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
