#include <stdio.h>
#include <stdlib.h>
#include <CUnit/Basic.h>
#include "../result_list.h"

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

/******************************************************************************/
//Copied from result_list.c
/**
 * The Bucket inside a node of the result list.
 * Contains a 2d array of row ids (uint64_t) and an index (unsigned int)
 * to the next empty space in the array.
 */
typedef struct result_list_bucket
{
    uint64_t row_ids[RESULT_LIST_BUCKET_SIZE][2];
    unsigned int index_to_add_next;
} result_list_bucket;

/**
 * The node of the result list.
 * Contains a bucket with the row ids and a pointer to the next node.
 */
typedef struct result_list_node
{
    result_list_bucket bucket;
    result_list_node* next;
} result_list_node;

/**
 * The result list.
 * Contains pointers to the head and tail nodes (for O(1) append)
 * and a node counter.
 */
typedef struct result_list
{
    result_list_node* head; //The first node of the list
    result_list_node* tail; //The last node of the list
    int number_of_nodes; //Counter of the buckets;
} result_list;
result_list_node* create_result_list_node();
void print_result_list(result_list*);
void print_bucket(result_list_bucket*);
int is_result_list_bucket_full(result_list_bucket* bucket);
int append_to_bucket(result_list_bucket* bucket, uint64_t r_row_id, uint64_t s_row_id);
/******************************************************************************/
//The following functions are not being tested here
//void print_bucket(result_list_bucket* bucket);
//void print_result_list(result_list*);
//void delete_result_list(result_list*);

void testCreate_result_list_node()
{
    result_list_node* list_node=create_result_list_node();
    //Check if the result list node is created and initialized correctly
    if(list_node==NULL||list_node->next!=NULL||list_node->bucket.index_to_add_next!=0)
    {
        CU_ASSERT(0);
        return;
    }
    //Check if the bucket size is correct
    if(sizeof(list_node->bucket.row_ids)!=RESULT_LIST_BUCKET_SIZE*(2*sizeof(uint64_t)))
    {
        CU_ASSERT(0);
    }
    free(list_node);
}

void testIs_result_list_bucket_full()
{
    //Create a result bucket
    result_list_bucket bucket;
    bucket.index_to_add_next=0;
    //Check the index when empty
    if(is_result_list_bucket_full(&bucket))
    {
        CU_ASSERT(0);
    }
    bucket.index_to_add_next=RESULT_LIST_BUCKET_SIZE;
    //Check the index when full
    if(!is_result_list_bucket_full(&bucket))
    {
        CU_ASSERT(0);
    }
}

void testAppend_to_bucket()
{
    //Initialize the testing variables
    result_list_bucket bucket;
    bucket.index_to_add_next=0;
    uint64_t r_row_id=1;
    uint64_t s_row_id=2;
    //Check the append operation
    unsigned int i=0;
    for(;i<RESULT_LIST_BUCKET_SIZE;i++)
    {
        if(append_to_bucket(&bucket, r_row_id, s_row_id)!=0||bucket.row_ids[i][ROWID_R_INDEX]!=r_row_id||bucket.row_ids[i][ROWID_S_INDEX]!=s_row_id)
        {
            CU_ASSERT(0);
        }
        r_row_id+=2;
        s_row_id+=2;
    }
    //Test if bucket is full (append to bucket must fail)
    if(append_to_bucket(&bucket, r_row_id, s_row_id)!=1&&bucket.index_to_add_next!=i)
    {
        CU_ASSERT(0);
    }
    print_bucket(&bucket);
}

void testCreate_result_list()
{
    result_list* list=create_result_list();
    if(list==NULL||list->head!=NULL||list->number_of_nodes!=0||list->tail!=NULL)
    {
        CU_ASSERT(0);
    }
    free(list);
}

void testAppend_to_list()
{
    result_list* list=create_result_list();
    if(list==NULL)
    {
        CU_ASSERT(0);
        return;
    }
    uint64_t r_row_id=1;
    uint64_t s_row_id=2;
    unsigned int i=0;
    for(;i<RESULT_LIST_BUCKET_SIZE;i++)
    {
        if(append_to_list(list, r_row_id, s_row_id)!=0||
                list->head->bucket.row_ids[i][ROWID_R_INDEX]!=r_row_id||
                list->head->bucket.row_ids[i][ROWID_S_INDEX]!=s_row_id||
                list->tail->bucket.row_ids[i][ROWID_R_INDEX]!=r_row_id||
                list->tail->bucket.row_ids[i][ROWID_S_INDEX]!=s_row_id)
        {
            CU_ASSERT(0);
        }
        r_row_id+=2;
        s_row_id+=2;
    }
    if(list->number_of_nodes!=1||list->head->bucket.index_to_add_next!=i||list->head!=list->tail)
    {
        CU_ASSERT(0);
    }
    for(i=0;i<RESULT_LIST_BUCKET_SIZE;i++)
    {
        if(append_to_list(list, r_row_id, s_row_id)!=0||list->tail->bucket.row_ids[i][ROWID_R_INDEX]!=r_row_id||list->tail->bucket.row_ids[i][ROWID_S_INDEX]!=s_row_id)
        {
            CU_ASSERT(0);
        }
        r_row_id+=2;
        s_row_id+=2;
    }
    if(list->number_of_nodes!=2||list->head->bucket.index_to_add_next!=i||list->tail->bucket.index_to_add_next!=i||list->head==list->tail)
    {
        CU_ASSERT(0);
    }
    print_result_list(list);
    delete_result_list(list);
}

void testIs_result_list_empty()
{
    result_list* list=create_result_list();
    if(list==NULL)
    {
        CU_ASSERT(0);
        return;
    }
    if(is_result_list_empty(list)!=1)
    {
        CU_ASSERT(0);
    }
    if(append_to_list(list, 1, 2)!=0||is_result_list_empty(list)!=0)
    {
        CU_ASSERT(0);
    }
    print_result_list(list);
    delete_result_list(list);
}

void testResult_list_get_number_of_buckets()
{
    result_list* list=create_result_list();
    if(list==NULL)
    {
        CU_ASSERT(0);
        return;
    }
    uint64_t r_row_id=1;
    uint64_t s_row_id=2;
    int i=0;
    for(;i<RESULT_LIST_BUCKET_SIZE*6;i++)
    {
        if(append_to_list(list, r_row_id, s_row_id)!=0||
                list->tail->bucket.row_ids[i%RESULT_LIST_BUCKET_SIZE][ROWID_R_INDEX]!=r_row_id||
                list->tail->bucket.row_ids[i%RESULT_LIST_BUCKET_SIZE][ROWID_S_INDEX]!=s_row_id)
        {
            CU_ASSERT(0);
        }
        r_row_id+=2;
        s_row_id+=2;
    }
    if(list->number_of_nodes!=6)
    {
        CU_ASSERT(0);
    }
    print_result_list(list);
    delete_result_list(list);
}

void testResult_list_get_number_of_records()
{
    result_list* list=create_result_list();
    if(list==NULL)
    {
        CU_ASSERT(0);
        return;
    }
    uint64_t r_row_id=1;
    uint64_t s_row_id=2;
    int i=0;
    for(;i<RESULT_LIST_BUCKET_SIZE*6;i++)
    {
        if(append_to_list(list, r_row_id, s_row_id)!=0||
                list->tail->bucket.row_ids[i%RESULT_LIST_BUCKET_SIZE][ROWID_R_INDEX]!=r_row_id||
                list->tail->bucket.row_ids[i%RESULT_LIST_BUCKET_SIZE][ROWID_S_INDEX]!=s_row_id)
        {
            CU_ASSERT(0);
        }
        r_row_id+=2;
        s_row_id+=2;
    }
    if(result_list_get_number_of_records(list)!=RESULT_LIST_BUCKET_SIZE*6)
    {
        CU_ASSERT(0);
    }
    print_result_list(list);
    delete_result_list(list);
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
    pSuite=CU_add_suite("result_list_test", init_suite, clean_suite);
    if(NULL==pSuite)
    {
        CU_cleanup_registry();
        return CU_get_error();
    }
    /* Add the tests to the suite */
    if((NULL==CU_add_test(pSuite, "testCreate_result_list_node", testCreate_result_list_node))||
            (NULL==CU_add_test(pSuite, "testIs_result_list_bucket_full", testIs_result_list_bucket_full))||
            (NULL==CU_add_test(pSuite, "testAppend_to_bucket", testAppend_to_bucket))||
            (NULL==CU_add_test(pSuite, "testCreate_result_list", testCreate_result_list))||
            (NULL==CU_add_test(pSuite, "testAppend_to_list", testAppend_to_list))||
            (NULL==CU_add_test(pSuite, "testIs_result_list_empty", testIs_result_list_empty))||
            (NULL==CU_add_test(pSuite, "testResult_list_get_number_of_buckets", testResult_list_get_number_of_buckets))||
            (NULL==CU_add_test(pSuite, "testResult_list_get_number_of_records", testResult_list_get_number_of_records)))
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
