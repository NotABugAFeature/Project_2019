#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <stdbool.h>
#include <CUnit/Basic.h>
#include "../queue.h"

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


void testCreate_queue()
{
    queue *q = create_queue();
    CU_ASSERT_EQUAL(q->head, NULL);
    CU_ASSERT_EQUAL(q->tail, NULL);
    free(q);
}

void testIs_empty1()
{
    queue *q = malloc(sizeof(queue));
    q->head = NULL;
    q->tail = NULL;
    CU_ASSERT_EQUAL(is_empty(q), true);
    free(q);
}

void testIs_empty2()
{
    queue *q = malloc(sizeof(queue));
    q->head = NULL;
    q->tail = NULL;

    q->head = malloc(sizeof(q_node));
    CU_ASSERT_EQUAL(is_empty(q), false);
    free(q->head);
    free(q);
}

void testPush1()
{
    queue *q = create_queue();
    window *w;
    int num = 10;
    for(int i=0; i<num; i++)
    {
        w = malloc(sizeof(window));
        w->start = i;
        push(q, w);
    }

    q_node *n = q->head;
    for(int i=0; i<num; i++)
    {
        CU_ASSERT_EQUAL(n->record->start, i);
        n = n->next;
    }

    for(int i=0; i<num; i++)
    {
        n = q->head;
        q->head = q->head->next;
        free(n->record);
        free(n);
    }

    free(q);
}

void testPush2()
{
    queue *q = create_queue();
    window *w;
    int num = 10000;
    for(int i=0; i<num; i++)
    {
        w = malloc(sizeof(window));
        w->start = i;
        push(q, w);
    }

    q_node *n = q->head;
    for(int i=0; i<num; i++)
    {
        CU_ASSERT_EQUAL(n->record->start, i);
        n = n->next;
    }

    for(int i=0; i<num; i++)
    {
        n = q->head;
        q->head = q->head->next;
        free(n->record);
        free(n);
    }

    free(q);
}

void testPop1()
{
    queue *q = create_queue();
    window *w;
    int num = 10;
    for(int i=0; i<num; i++)
    {
        w = malloc(sizeof(window));
        w->start = i;
        push(q, w);
    }
    
    for(int i=0; i<num; i++)
    {
        w = pop(q);
        CU_ASSERT_EQUAL(w->start, i);
        free(w);
    }
    
    CU_ASSERT_EQUAL(q->head, NULL);
    CU_ASSERT_EQUAL(q->tail, NULL);
    free(q);
}

void testPop2()
{
    queue *q = create_queue();
    window *w;
    int num = 1000;
    for(int i=0; i<num; i++)
    {
        w = malloc(sizeof(window));
        w->start = i;
        push(q, w);
    }
    
    for(int i=0; i<num; i++)
    {
        w = pop(q);
        CU_ASSERT_EQUAL(w->start, i);
        free(w);
    }
    
    CU_ASSERT_EQUAL(q->head, NULL);
    CU_ASSERT_EQUAL(q->tail, NULL);
    free(q);
}


int main(void)
{
    CU_pSuite pSuite=NULL;

    /* Initialize the CUnit test registry */
    if(CUE_SUCCESS!=CU_initialize_registry())
    {
        return CU_get_error();
    }

    /* Add a suite to the registry */
    pSuite=CU_add_suite("queue_test", init_suite, clean_suite);
    if(NULL==pSuite)
    {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* Add the tests to the suite */
    if( (NULL==CU_add_test(pSuite, "testCreate_queue", testCreate_queue))||
        (NULL==CU_add_test(pSuite, "testIs_empty1", testIs_empty1))||
        (NULL==CU_add_test(pSuite, "testIs_empty2", testIs_empty2))||
        (NULL==CU_add_test(pSuite, "testPush1", testPush1))||
        (NULL==CU_add_test(pSuite, "testPush2", testPush2))||
        (NULL==CU_add_test(pSuite, "testPop1", testPop1))||
        (NULL==CU_add_test(pSuite, "testPop2", testPop2))
      )
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
