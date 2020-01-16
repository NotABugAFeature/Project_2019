#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <CUnit/Basic.h>
#include "list_array.h"
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

void testCreate_list_array()
{
    unsigned int size, lists;
    //size or lists is 0 - wrong parameters
    size=0; lists=1;
    CU_ASSERT_EQUAL(create_list_array(size, lists), NULL);
    size=5; lists=0;
    CU_ASSERT_EQUAL(create_list_array(size, lists), NULL);
    //lists > 2 - wrongs parameters
    size=5; lists=3;
    CU_ASSERT_EQUAL(create_list_array(size, lists), NULL);

    //Successful
    size=5; lists=2;
    list_array *la = create_list_array(size, lists);
    CU_ASSERT_NOT_EQUAL_FATAL(la, NULL);
    CU_ASSERT_EQUAL(la->num_lists, size);
    CU_ASSERT_NOT_EQUAL_FATAL(la->lists, NULL);
    for(unsigned int i=0; i<size; i++)
    {
        CU_ASSERT_NOT_EQUAL_FATAL(la->lists[i], NULL);
        for(unsigned int j=0; j<lists; j++)
        {
            CU_ASSERT_NOT_EQUAL(la->lists[i][j], NULL);
            delete_middle_list(la->lists[i][j]);
        }
    }

    delete_list_array(la);
}

void testAppend_middle_list()
{
    middle_list *a, *b;

    //a or b is NULL - nothing is done
    a=NULL; b=NULL;
    CU_ASSERT_EQUAL(a, NULL);
    a=NULL; b=create_middle_list();
    CU_ASSERT_EQUAL(a, NULL);

    //Successful
    a=create_middle_list();
    middle_list_node *node;
    int a_size = 20, b_size = 10;

    //Create two lists with 20 and 10 buckets
    a->head = NULL;
    a->number_of_nodes = a_size;
    middle_list_node **next = &(a->head);
    for(int i=0; i<a_size; i++)
    {
        node = malloc(sizeof(middle_list_node));
        node->bucket.index_to_add_next = i;
        *next = node;
        next = &(node->next);
    }
    a->tail = node;

    b->head = NULL;
    b->number_of_nodes = b_size;
    next = &(b->head);
    for(int i=0; i<b_size; i++)
    {
        node = malloc(sizeof(middle_list_node));
        node->bucket.index_to_add_next = a_size + i;
        *next = node;
        next = &(node->next);
    }
    b->tail = node;

    //keep start and end to check later
    middle_list_node *a_head = a->head;
    middle_list_node *b_tail = b->tail;

    append_middle_list(a, b);
    CU_ASSERT_EQUAL(a->number_of_nodes, a_size+b_size);
    CU_ASSERT_EQUAL(a->head, a_head);
    CU_ASSERT_EQUAL(a->tail, b_tail);
    node = a->head;
    for(int i=0; i<a_size+b_size; i++)
    {
        CU_ASSERT_EQUAL(node->bucket.index_to_add_next, i);
        node = node->next;
    }

    delete_middle_list(a);
}


void testMerge_middle_lists()
{
    list_array *la = NULL;
    middle_list *a=NULL, *b=NULL;

    //la, a, b NULL - NULL parameters, nothing happens
    merge_middle_lists(la, a, b);
    CU_ASSERT_EQUAL(a, NULL);
    CU_ASSERT_EQUAL(b, NULL);

    //Successful
    unsigned int size = 3, lists = 2, buckets = 5;
    la = create_list_array(size, lists);
    a = create_middle_list();
    b = create_middle_list();

    middle_list_node **next, *node;

    for(unsigned int i=0; i<size; i++)
    {
        //lists in position 0
        la->lists[i][0]->head = NULL;
        la->lists[i][0]->number_of_nodes = buckets;
        next = &(la->lists[i][0]->head);
        for(unsigned int j=0; j<buckets; j++)
        {
            node = malloc(sizeof(middle_list_node));
            *next = node;
            next = &(node->next);
        }
        la->lists[i][0]->tail = node;

        //lists in position 1
        la->lists[i][1]->head = NULL;
        la->lists[i][1]->number_of_nodes = buckets;
        next = &(la->lists[i][1]->head);
        for(unsigned int j=0; j<buckets; j++)
        {
            node = malloc(sizeof(middle_list_node));
            *next = node;
            next = &(node->next);
        }
        la->lists[i][1]->tail = node;
    }
    middle_list_node *start_a = la->lists[0][0]->head, *start_b = la->lists[0][1]->head;
    middle_list_node *end_a = la->lists[size-1][0]->tail, *end_b = la->lists[size-1][1]->tail;

    merge_middle_lists(la, a, b);
    CU_ASSERT_EQUAL(a->number_of_nodes, size*buckets);
    CU_ASSERT_EQUAL(b->number_of_nodes, size*buckets);
    CU_ASSERT_EQUAL(a->head, start_a);
    CU_ASSERT_EQUAL(a->tail, end_a);
    CU_ASSERT_EQUAL(b->head, start_b);
    CU_ASSERT_EQUAL(b->tail, end_b);

    delete_middle_list(a);
    delete_middle_list(b);
    delete_list_array(la);
}

void testMerge_middle_list()
{
    list_array *la = NULL;
    middle_list *a=NULL;

    //la, a NULL - NULL parameters, nothing happens
    merge_middle_list(la, a);
    CU_ASSERT_EQUAL(a, NULL);

    //Successful
    unsigned int size = 3, lists = 1, buckets = 5;
    la = create_list_array(size, lists);
    a = create_middle_list();

    middle_list_node **next, *node;

    for(unsigned int i=0; i<size; i++)
    {
        //lists in position 0
        la->lists[i][0]->head = NULL;
        la->lists[i][0]->number_of_nodes = buckets;
        next = &(la->lists[i][0]->head);
        for(unsigned int j=0; j<buckets; j++)
        {
            node = malloc(sizeof(middle_list_node));
            *next = node;
            next = &(node->next);
        }
        la->lists[i][0]->tail = node;
    }
    middle_list_node *start_a = la->lists[0][0]->head;
    middle_list_node *end_a = la->lists[size-1][0]->tail;

    merge_middle_list(la, a);
    CU_ASSERT_EQUAL(a->number_of_nodes, size*buckets);
    CU_ASSERT_EQUAL(a->head, start_a);
    CU_ASSERT_EQUAL(a->tail, end_a);

    delete_middle_list(a);
    delete_list_array(la);
}


int main()
{
    CU_pSuite pSuite=NULL;

    /* Initialize the CUnit test registry */
    if(CUE_SUCCESS!=CU_initialize_registry())
        return CU_get_error();

    /* Add a suite to the registry */
    pSuite=CU_add_suite("list_array", init_suite, clean_suite);
    if(NULL==pSuite)
    {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* Add the tests to the suite */
    if((NULL==CU_add_test(pSuite, "testCreate_list_array", testCreate_list_array))||
       (NULL==CU_add_test(pSuite, "testAppend_middle_list", testAppend_middle_list))||
       (NULL==CU_add_test(pSuite, "testMerge_middle_lists", testMerge_middle_lists))||
       (NULL==CU_add_test(pSuite, "testMerge_middle_list", testMerge_middle_list)))
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
