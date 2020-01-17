#include <stdio.h>
#include <stdlib.h>
#include <CUnit/Basic.h>
#include "../projection_list.h"
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
typedef struct projection_node
{
    uint64_t query_id; //The query id (used for sorting the nodes)
    uint32_t number_of_projections; //The number of projections of the query
    uint64_t* projections; //The array of the projection results
    projection_node* next; //Pointer to the next node
} projection_node;
projection_node* create_projection_node(uint64_t query_id, uint32_t number_of_projections, uint32_t projection_index, uint64_t projection_sum);
void delete_projection_node(projection_node* node);
void testCreate_projection_node()
{
    uint64_t query_id=1;
    uint32_t number_of_projections=3;
    uint32_t projection_index=1;
    uint64_t projection_sum=1000;
    projection_node* new_node=create_projection_node(query_id, number_of_projections, projection_index, projection_sum);
    CU_ASSERT_NOT_EQUAL_FATAL(new_node,NULL);
    CU_ASSERT_EQUAL(new_node->next,NULL);
    CU_ASSERT_EQUAL(new_node->query_id,query_id);
    CU_ASSERT_EQUAL(new_node->number_of_projections,number_of_projections);
    CU_ASSERT_EQUAL(new_node->projections[projection_index],projection_sum);
    CU_ASSERT_EQUAL(new_node->projections[0],0);
    CU_ASSERT_EQUAL(new_node->projections[2],0);
    delete_projection_node(new_node);
}
void testCreate_projection_list()
{
    projection_list* new_list=create_projection_list();
    CU_ASSERT_NOT_EQUAL_FATAL(new_list,NULL);
    CU_ASSERT_EQUAL(new_list->head,NULL);
    CU_ASSERT_EQUAL(new_list->number_of_nodes,0);
    CU_ASSERT_EQUAL(new_list->tail,0);
    delete_projection_list(new_list);
}
void testAppend_to_projection_list()
{
    projection_list* p_list=create_projection_list();
    CU_ASSERT_NOT_EQUAL_FATAL(p_list,NULL);
    uint64_t query_id=2;
    uint32_t number_of_projections=4;
    uint64_t projection_sum=1000;
    for(uint32_t i=0;i<number_of_projections;i++)
    {
        CU_ASSERT_EQUAL(append_to_projection_list(p_list,query_id,number_of_projections,i,projection_sum+i*projection_sum),0);
    }
    CU_ASSERT_EQUAL(p_list->head->query_id,query_id);
    CU_ASSERT_EQUAL(p_list->head->number_of_projections,number_of_projections);
    CU_ASSERT_EQUAL(p_list->head,p_list->tail);
    CU_ASSERT_EQUAL(p_list->head->next,NULL);
    CU_ASSERT_EQUAL(p_list->tail->next,NULL);
    for(uint32_t i=0;i<number_of_projections;i++)
    {
        CU_ASSERT_EQUAL(p_list->head->projections[i],projection_sum+i*projection_sum);
    }
    for(uint32_t i=0;i<number_of_projections+1;i++)
    {
        CU_ASSERT_EQUAL(append_to_projection_list(p_list,query_id+2,number_of_projections+1,i,projection_sum*10+i*projection_sum),0);
    }
    CU_ASSERT_EQUAL(p_list->head->query_id,query_id);
    CU_ASSERT_EQUAL(p_list->tail->query_id,query_id+2);
    CU_ASSERT_EQUAL(p_list->head->number_of_projections,number_of_projections);
    CU_ASSERT_EQUAL(p_list->tail->number_of_projections,number_of_projections+1);
    CU_ASSERT_EQUAL(p_list->head->next,p_list->tail);
    CU_ASSERT_EQUAL(p_list->tail->next,NULL);
    for(uint32_t i=0;i<number_of_projections;i++)
    {
        CU_ASSERT_EQUAL(p_list->head->projections[i],projection_sum+i*projection_sum);
    }
    for(uint32_t i=0;i<number_of_projections+1;i++)
    {
        CU_ASSERT_EQUAL(p_list->tail->projections[i],projection_sum*10+i*projection_sum);
    }
    for(uint32_t i=0;i<number_of_projections+2;i++)
    {
        CU_ASSERT_EQUAL(append_to_projection_list(p_list,query_id+1,number_of_projections+2,i,projection_sum*100+i*projection_sum),0);
    }
    CU_ASSERT_EQUAL(p_list->head->query_id,query_id);
    CU_ASSERT_EQUAL(p_list->head->next->query_id,query_id+1);
    CU_ASSERT_EQUAL(p_list->head->next->next,p_list->tail);
    CU_ASSERT_EQUAL(p_list->tail->query_id,query_id+2);
    CU_ASSERT_EQUAL(p_list->head->number_of_projections,number_of_projections);
    CU_ASSERT_EQUAL(p_list->head->next->number_of_projections,number_of_projections+2);
    CU_ASSERT_EQUAL(p_list->tail->number_of_projections,number_of_projections+1);
    CU_ASSERT_EQUAL(p_list->tail->next,NULL);
    for(uint32_t i=0;i<number_of_projections;i++)
    {
        CU_ASSERT_EQUAL(p_list->head->projections[i],projection_sum+i*projection_sum);
    }
    for(uint32_t i=0;i<number_of_projections+1;i++)
    {
        CU_ASSERT_EQUAL(p_list->tail->projections[i],projection_sum*10+i*projection_sum);
    }
    for(uint32_t i=0;i<number_of_projections+2;i++)
    {
        CU_ASSERT_EQUAL(p_list->head->next->projections[i],projection_sum*100+i*projection_sum);
    }
    for(uint32_t i=0;i<number_of_projections+3;i++)
    {
        CU_ASSERT_EQUAL(append_to_projection_list(p_list,query_id-1,number_of_projections+3,i,projection_sum*1000+i*projection_sum),0);
    }
    CU_ASSERT_EQUAL(p_list->head->query_id,query_id-1);
    CU_ASSERT_EQUAL(p_list->head->next->query_id,query_id);
    CU_ASSERT_EQUAL(p_list->head->next->next->query_id,query_id+1);
    CU_ASSERT_EQUAL(p_list->head->next->next->next,p_list->tail);
    CU_ASSERT_EQUAL(p_list->tail->query_id,query_id+2);
    CU_ASSERT_EQUAL(p_list->head->number_of_projections,number_of_projections+3);
    CU_ASSERT_EQUAL(p_list->head->next->number_of_projections,number_of_projections);
    CU_ASSERT_EQUAL(p_list->head->next->next->number_of_projections,number_of_projections+2);
    CU_ASSERT_EQUAL(p_list->tail->number_of_projections,number_of_projections+1);
    CU_ASSERT_EQUAL(p_list->tail->next,NULL);
    for(uint32_t i=0;i<number_of_projections+3;i++)
    {
        CU_ASSERT_EQUAL(p_list->head->projections[i],projection_sum*1000+i*projection_sum);
    }
    for(uint32_t i=0;i<number_of_projections;i++)
    {
        CU_ASSERT_EQUAL(p_list->head->next->projections[i],projection_sum+i*projection_sum);
    }
    for(uint32_t i=0;i<number_of_projections+1;i++)
    {
        CU_ASSERT_EQUAL(p_list->tail->projections[i],projection_sum*10+i*projection_sum);
    }
    for(uint32_t i=0;i<number_of_projections+2;i++)
    {
        CU_ASSERT_EQUAL(p_list->head->next->next->projections[i],projection_sum*100+i*projection_sum);
    }
    delete_projection_list(p_list);
}
int main()
{
    CU_pSuite pSuite=NULL;

    /* Initialize the CUnit test registry */
    if(CUE_SUCCESS!=CU_initialize_registry())
        return CU_get_error();

    /* Add a suite to the registry */
    pSuite=CU_add_suite("projection_list_test", init_suite, clean_suite);
    if(NULL==pSuite)
    {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* Add the tests to the suite */
    if((NULL==CU_add_test(pSuite, "testCreate_projection_node", testCreate_projection_node))||
       (NULL==CU_add_test(pSuite, "testCreate_projection_list", testCreate_projection_list))||
       (NULL==CU_add_test(pSuite, "testAppend_to_projection_list", testAppend_to_projection_list)))
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
