#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <CUnit/CUnit.h>
#include <CUnit/Basic.h>
#include <limits.h>
#include "radix_sort.h"
#include "sort_merge_join.h"
#include "queue.h"
#include "table.h"
#include "string_list.h"
#include "execute_query.h"
#include "query.h"
#include "middle_list.h"
#include "job_fifo.h"
#include "list_array.h"
#include "projection_list.h"

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


/*
 * radix_sort.c
 */


void testCreate_histogram1()
{
    relation p2;

    p2.num_tuples = 3;
    p2.tuples = malloc(3*sizeof(tuple));
    CU_ASSERT_NOT_EQUAL(p2.tuples, NULL);

    p2.tuples[0].key = 1;
    p2.tuples[0].row_id = 1;

    p2.tuples[1].key = 1;
    p2.tuples[1].row_id = 2;

    p2.tuples[2].key = 3;
    p2.tuples[2].row_id = 3;

    uint64_t *hist = malloc(HIST_SIZE*sizeof(uint64_t));
    CU_ASSERT_NOT_EQUAL(hist, NULL);

    for(uint64_t i=0; i<HIST_SIZE; i++)
    {
        hist[i] = 0;
    }

    int res = create_histogram(&p2, 0, 3, hist, 8);

    CU_ASSERT_EQUAL(res, 0);

    CU_ASSERT_EQUAL(hist[1], 2);
    CU_ASSERT_EQUAL(hist[3], 1);

    CU_ASSERT_EQUAL(hist[0], 0);
    CU_ASSERT_EQUAL(hist[2], 0);

    for(uint64_t i=4; i<HIST_SIZE; i++)
        CU_ASSERT_EQUAL(hist[i], 0);

    free(p2.tuples);
    free(hist);
}


void testCreate_histogram2()
{
    uint64_t *hist = malloc(HIST_SIZE*sizeof(uint64_t));
    CU_ASSERT_NOT_EQUAL(hist, NULL);

    int res = create_histogram(NULL, 0, 3, hist, 8);

    CU_ASSERT_EQUAL(res, 1);

    free(hist);
}


void testCreate_histogram3()
{
    relation p2;p2.num_tuples=0;p2.tuples=NULL;

    int res = create_histogram(&p2, 0, 3, NULL, 8);

    CU_ASSERT_EQUAL(res, 1);
}


void testCreate_histogram4()
{
    relation p2;p2.num_tuples=0;p2.tuples=NULL;

    int res = create_histogram(&p2, 0, 3, NULL, 8);

    CU_ASSERT_EQUAL(res, 1);
}


void testCreate_histogram5()
{
    relation p2;

    p2.num_tuples = 3;
    p2.tuples = malloc(3*sizeof(tuple));
    CU_ASSERT_NOT_EQUAL(p2.tuples, NULL);

    p2.tuples[0].key = 1;
    p2.tuples[0].row_id = 1;

    p2.tuples[1].key = 1;
    p2.tuples[1].row_id = 2;

    p2.tuples[2].key = 3;
    p2.tuples[2].row_id = 3;

    uint64_t *hist = malloc(HIST_SIZE*sizeof(uint64_t));
    CU_ASSERT_NOT_EQUAL(hist, NULL);

    for(uint64_t i=0; i<HIST_SIZE; i++)
    {
        hist[i] = 0;
    }

    int res = create_histogram(&p2, 0, 3, hist, 1);

    CU_ASSERT_EQUAL(res, 0);

    CU_ASSERT_EQUAL(hist[0], 3);

    for(uint64_t i=1; i<HIST_SIZE; i++)
        CU_ASSERT_EQUAL(hist[i], 0);

    free(p2.tuples);
    free(hist);
}


void testTransform_to_psum1()
{
    int res = transform_to_psum(NULL);
    CU_ASSERT_EQUAL(res, 1);
}


void testTransform_to_psum2()
{
    relation p2;

    p2.num_tuples = 3;
    p2.tuples = malloc(3*sizeof(tuple));
    CU_ASSERT_NOT_EQUAL(p2.tuples, NULL);

    p2.tuples[0].key = 1;
    p2.tuples[0].row_id = 1;

    p2.tuples[1].key = 1;
    p2.tuples[1].row_id = 2;

    p2.tuples[2].key = 3;
    p2.tuples[2].row_id = 3;

    uint64_t *hist = malloc(HIST_SIZE*sizeof(uint64_t));
    CU_ASSERT_NOT_EQUAL(hist, NULL);

    for(uint64_t i=0; i<HIST_SIZE; i++)
    {
        hist[i] = 0;
    }

    int res = create_histogram(&p2, 0, 3, hist, 8);
    CU_ASSERT_EQUAL(res, 0);

    res = transform_to_psum(hist);
    CU_ASSERT_EQUAL(res, 0);

    CU_ASSERT_EQUAL(hist[0], 0);
    CU_ASSERT_EQUAL(hist[1], 0);
    CU_ASSERT_EQUAL(hist[2], 2);
    CU_ASSERT_EQUAL(hist[3], 2);
    CU_ASSERT_EQUAL(hist[4], 3);

    for(uint64_t i=5; i<HIST_SIZE; i++)
        CU_ASSERT_EQUAL(hist[i], 3);

    free(p2.tuples);
    free(hist);
}


void copy_relation(relation* source, relation* target, uint64_t start_index, uint64_t end_index);

void testCopy_relation()
{
    relation p1, p2;

    p2.num_tuples = 3;
    p2.tuples = malloc(3*sizeof(tuple));
    CU_ASSERT_NOT_EQUAL(p2.tuples, NULL);

    p2.tuples[0].key = 1;
    p2.tuples[0].row_id = 1;

    p2.tuples[1].key = 1;
    p2.tuples[1].row_id = 2;

    p2.tuples[2].key = 3;
    p2.tuples[2].row_id = 3;

    p1.num_tuples = p2.num_tuples;
    p1.tuples = malloc(3*sizeof(tuple));
    CU_ASSERT_NOT_EQUAL(p1.tuples, NULL);

    copy_relation(&p2, &p1, 0, 3);

    CU_ASSERT_EQUAL(p2.tuples[0].key, p1.tuples[0].key);
    CU_ASSERT_EQUAL(p2.tuples[0].row_id, p1.tuples[0].row_id);

    CU_ASSERT_EQUAL(p2.tuples[1].key, p1.tuples[1].key);
    CU_ASSERT_EQUAL(p2.tuples[1].row_id, p1.tuples[1].row_id);

    CU_ASSERT_EQUAL(p2.tuples[2].key, p1.tuples[2].key);
    CU_ASSERT_EQUAL(p2.tuples[2].row_id, p1.tuples[2].row_id);

    free(p2.tuples);
    free(p1.tuples);
}


void testCopy_relation_with_psum1()
{
    relation target;target.num_tuples=0;target.tuples=NULL;
    uint64_t index_start=0;
    uint64_t index_end=0;
    uint64_t *psum = malloc(HIST_SIZE*sizeof(uint64_t));
    unsigned short nbyte=1;

    int result = copy_relation_with_psum(NULL, &target, index_start, index_end, psum, nbyte);
    CU_ASSERT_EQUAL(result, 1);
    free(psum);
}


void testCopy_relation_with_psum2()
{
    relation source;source.num_tuples=0;source.tuples=NULL;
    uint64_t index_start=0;
    uint64_t index_end=0;
    uint64_t *psum = malloc(HIST_SIZE*sizeof(uint64_t));
    unsigned short nbyte=1;

    int result = copy_relation_with_psum(&source, NULL, index_start, index_end, psum, nbyte);
    CU_ASSERT_EQUAL(result, 1);
    free(psum);
}


void testCopy_relation_with_psum3()
{
    relation source;source.num_tuples=0;source.tuples=NULL;
    relation target;target.num_tuples=0;target.tuples=NULL;
    uint64_t index_start=0;
    uint64_t index_end=0;
    unsigned short nbyte=1;

    int result = copy_relation_with_psum(&source, &target, index_start, index_end, NULL, nbyte);
    CU_ASSERT_EQUAL(result, 1);
}


void testCopy_relation_with_psum4()
{
    relation p1, p2;

    p2.num_tuples = 3;
    p2.tuples = malloc(3*sizeof(tuple));
    CU_ASSERT_NOT_EQUAL(p2.tuples, NULL);

    p2.tuples[0].key = 1;
    p2.tuples[0].row_id = 1;

    p2.tuples[1].key = 3;
    p2.tuples[1].row_id = 2;

    p2.tuples[2].key = 1;
    p2.tuples[2].row_id = 3;

    p1.num_tuples = p2.num_tuples;
    p1.tuples = malloc((p1.num_tuples)*sizeof(tuple));
    CU_ASSERT_NOT_EQUAL(p1.tuples, NULL);

    uint64_t *hist = malloc(HIST_SIZE*sizeof(uint64_t));
    CU_ASSERT_NOT_EQUAL(hist, NULL);

    for(uint64_t i=0; i<HIST_SIZE; i++)
    {
        hist[i] = 0;
    }

    int res = create_histogram(&p2, 0, 3, hist, 8);
    CU_ASSERT_EQUAL(res, 0);

    res = transform_to_psum(hist);
    CU_ASSERT_EQUAL(res, 0);

    res = copy_relation_with_psum(&p2, &p1, 0, 3, hist, 8);
    CU_ASSERT_EQUAL(res, 0);

    CU_ASSERT_EQUAL(p1.tuples[0].key, 1);
    CU_ASSERT_EQUAL(p1.tuples[1].key, 1);
    CU_ASSERT_EQUAL(p1.tuples[2].key, 3);

    free(p2.tuples);
    free(p1.tuples);
    free(hist);
}


void testRadix_sort1()
{
    relation p2;

    p2.num_tuples = 3;
    p2.tuples = malloc(3*sizeof(tuple));
    CU_ASSERT_NOT_EQUAL(p2.tuples, NULL);

    p2.tuples[0].key = 4;
    p2.tuples[0].row_id = 1;

    p2.tuples[1].key = 1;
    p2.tuples[1].row_id = 2;

    p2.tuples[2].key = 3;
    p2.tuples[2].row_id = 3;

    int res = radix_sort(&p2);

    CU_ASSERT_EQUAL(res, 0);
    CU_ASSERT_EQUAL(p2.tuples[0].key, 1);
    CU_ASSERT_EQUAL(p2.tuples[1].key, 3);
    CU_ASSERT_EQUAL(p2.tuples[2].key, 4);

    free(p2.tuples);
}


void testRadix_sort2()
{
    relation p2;

    p2.num_tuples = 500000;
    p2.tuples = malloc((p2.num_tuples)*sizeof(tuple));
    CU_ASSERT_NOT_EQUAL(p2.tuples, NULL);

    uint64_t st = 4647856104846919667;
    for(uint64_t i = 0; i < p2.num_tuples; i++)
    {
        p2.tuples[i].key = st;
        st--;
        p2.tuples[i].row_id = i;
    }

    int res = radix_sort(&p2);
    CU_ASSERT_EQUAL(res, 0);

    for(uint64_t i = 0; i < p2.num_tuples-1; i++)
    {
        CU_ASSERT(p2.tuples[i].key <= p2.tuples[i+1].key);
    }

    free(p2.tuples);
}


/*
 * relation.c
 */


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
	free(rel->tuples);
	free(rel);
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
	free(rel->tuples);
	free(rel);
}

//TODO: relation_to_file


/*
 * queue.c
 */


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


/*
 * quicksort.c
 */


 void testSwap()
 {
     //Create two tuples
     tuple t1;
     t1.key=1;
     t1.row_id=101;
     tuple t2;
     t2.key=2;
     t2.row_id=202;
     //Swap them
     swap(&t1, &t2);
     //Check their values
     if(t1.key!=2||t1.row_id!=202||t2.key!=1||t2.row_id!=101)
     {
         CU_ASSERT(0);
     }
 }

 void testPartition()
 {
     int size_of_test=10;
     //Create an array of tuples
     tuple test_tuples[size_of_test];
     //Initialize it with descending values
     for(int i=0;i<10;i++)
     {
         test_tuples[i].key=size_of_test-i;
         test_tuples[i].row_id=10*size_of_test-i;
     }
     print_tuples(test_tuples,size_of_test);
     //Call partition for the array
     uint64_t result=partition(test_tuples, 0, size_of_test-1);
     print_tuples(test_tuples,size_of_test);
     //Check if the first element has swapped with last and the result is 0
     if(result!=0||test_tuples[0].key!=1||test_tuples[size_of_test-1].key!=10)
     {
         CU_ASSERT(0);
     }
     //Re-Initialize it with alternating small and big values
     for(int i=0;i<size_of_test;i++)
     {
         if(i%2==0)
         {
             test_tuples[i].key=size_of_test-i;
             test_tuples[i].row_id=10*size_of_test-i;
         }
         else
         {
             test_tuples[i].key=5000*size_of_test-i;
             test_tuples[i].row_id=50000*size_of_test-i;
         }
     }
     print_tuples(test_tuples,size_of_test);
     //Call partition for the array
     result=partition(test_tuples, 0,size_of_test-1);
     print_tuples(test_tuples,size_of_test);
     //Check if the tuples from 0 to result are < tuple[result]
     for(int i=0;i<result;i++)
     {
         if(test_tuples[i].key>test_tuples[result].key)
         {
             CU_ASSERT(0);
         }
     }
     //Check if the tuples from result to end are > tuple[result]
     for(int i=result;i<size_of_test-1;i++)
     {
         if(test_tuples[i].key<test_tuples[result].key)
         {
             CU_ASSERT(0);
         }
     }
 }

 void testQuicksort()
 {
     int size_of_test=10;
     //Create an array of tuples
     tuple test_tuples[size_of_test];
     //Initialize it with descending values
     for(int i=0;i<10;i++)
     {
         test_tuples[i].key=size_of_test-i;
         test_tuples[i].row_id=10*size_of_test-i;
     }
     print_tuples(test_tuples,size_of_test);
     //Call quicksort for the array
     quicksort(test_tuples, 0, size_of_test-1);
     print_tuples(test_tuples,size_of_test);
     //Check if the array is sorted
     for(int i=0;i<size_of_test-2;i++)
     {
         if(test_tuples[i].key>test_tuples[i+1].key)
         {
             CU_ASSERT(0);
         }
     }
     //Re-Initialize it with alternating small and big values
     for(int i=0;i<size_of_test;i++)
     {
         if(i%2==0)
         {
             test_tuples[i].key=size_of_test-i;
             test_tuples[i].row_id=10*size_of_test-i;
         }
         else
         {
             test_tuples[i].key=5000*size_of_test-i;
             test_tuples[i].row_id=50000*size_of_test-i;
         }
     }
     print_tuples(test_tuples,size_of_test);
     //Call quicksort for the array
     quicksort(test_tuples, 0,size_of_test-1);
     print_tuples(test_tuples,size_of_test);
     //Check if the array is sorted
     for(int i=0;i<size_of_test-2;i++)
     {
         if(test_tuples[i].key>test_tuples[i+1].key)
         {
             CU_ASSERT(0);
         }
     }
 }


/*
 * sort_merge_join.c
 */

 /**
  * Test case 1. Relation t or relation s is NULL
  *
  */
 void testFinal_join1()
 {
     middle_list* list_t = create_middle_list();
     middle_list* list_s = create_middle_list();
     relation p2;

     int res = final_join(list_t, list_s, NULL, &p2);
     CU_ASSERT_EQUAL(res, 1);

     res = final_join(list_t, list_s, &p2, NULL);
     CU_ASSERT_EQUAL(res, 1);

     delete_middle_list(list_t);
     delete_middle_list(list_s);
 }


 /**
  * Test case 2. list_t or list_s is NULL
  *
  */
 void testFinal_join2()
 {
     relation p1, p2;
     middle_list *list = create_middle_list();

     int res = final_join(NULL, list, &p1, &p2);
     CU_ASSERT_EQUAL(res, 1);

     res = final_join(list, NULL, &p1, &p2);
     CU_ASSERT_EQUAL(res, 1);
     delete_middle_list(list);
 }


 
 /**
  * Test case 3. Successful test
  *
  */
 void testFinal_join3()
 {
     middle_list* list_t = create_middle_list();
     middle_list* list_s = create_middle_list();

     relation p1, p2;

     p1.num_tuples = 4;
     p1.tuples = malloc(4*sizeof(tuple));
     p1.tuples[0].key = 1;
     p1.tuples[0].row_id = 1;

     p1.tuples[1].key = 2;
     p1.tuples[1].row_id = 2;

     p1.tuples[2].key = 3;
     p1.tuples[2].row_id = 3;

     p1.tuples[3].key = 4;
     p1.tuples[3].row_id = 4;


     p2.num_tuples = 3;
     p2.tuples = malloc(3*sizeof(tuple));
     p2.tuples[0].key = 1;
     p2.tuples[0].row_id = 1;

     p2.tuples[1].key = 1;
     p2.tuples[1].row_id = 2;

     p2.tuples[2].key = 3;
     p2.tuples[2].row_id = 3;


     int res = final_join(list_t, list_s, &p1, &p2);

     CU_ASSERT_EQUAL(res, 0);
     CU_ASSERT_EQUAL(middle_list_get_number_of_records(list_t), 3);
     CU_ASSERT_EQUAL(middle_list_get_number_of_records(list_s), 3);

     free(p1.tuples);
     free(p2.tuples);
     delete_middle_list(list_t);
     delete_middle_list(list_s);
 }


 /**
  * Test case 4. Successful test
  *
  */
 void testFinal_join4()
 {
     middle_list* list_t = create_middle_list();
     middle_list* list_s = create_middle_list();

     relation p1, p2;

     p1.num_tuples = 4;
     p1.tuples = malloc(4*sizeof(tuple));
     p1.tuples[0].key = 6;
     p1.tuples[0].row_id = 1;

     p1.tuples[1].key = 8;
     p1.tuples[1].row_id = 2;

     p1.tuples[2].key = 39;
     p1.tuples[2].row_id = 3;

     p1.tuples[3].key = 46;
     p1.tuples[3].row_id = 4;


     p2.num_tuples = 3;
     p2.tuples = malloc(3*sizeof(tuple));
     p2.tuples[0].key = 1;
     p2.tuples[0].row_id = 1;

     p2.tuples[1].key = 1;
     p2.tuples[1].row_id = 2;

     p2.tuples[2].key = 3;
     p2.tuples[2].row_id = 3;


     int res = final_join(list_t, list_s, &p1, &p2);

     CU_ASSERT_EQUAL(res, 0);
     CU_ASSERT_EQUAL(middle_list_get_number_of_records(list_t), 0);
     CU_ASSERT_EQUAL(middle_list_get_number_of_records(list_s), 0);

     free(p1.tuples);
     free(p2.tuples);
     delete_middle_list(list_t);
     delete_middle_list(list_s);
 }


 /**
  * Test case 5. Successful test
  *
  */
 void testFinal_join5()
 {
     middle_list* list_t = create_middle_list();
     middle_list* list_s = create_middle_list();

     relation p1, p2;

     p1.num_tuples = 4;
     p1.tuples = malloc(4*sizeof(tuple));
     p1.tuples[0].key = 2251799813684550;
     p1.tuples[0].row_id = 1;

     p1.tuples[1].key = 2251799813684730;
     p1.tuples[1].row_id = 2;

     p1.tuples[2].key = 2251799813684860;
     p1.tuples[2].row_id = 3;

     p1.tuples[3].key = 4647856104846919671;
     p1.tuples[3].row_id = 4;


     p2.num_tuples = 3;
     p2.tuples = malloc(3*sizeof(tuple));
     p2.tuples[0].key = 1;
     p2.tuples[0].row_id = 1;

     p2.tuples[1].key = 2251799813684550;
     p2.tuples[1].row_id = 2;

     p2.tuples[2].key = 4647856104846919671;
     p2.tuples[2].row_id = 3;


     int res = final_join(list_t, list_s, &p1, &p2);

     CU_ASSERT_EQUAL(res, 0);
     CU_ASSERT_EQUAL(middle_list_get_number_of_records(list_t), 2);
     CU_ASSERT_EQUAL(middle_list_get_number_of_records(list_s), 2);

     free(p1.tuples);
     free(p2.tuples);
     delete_middle_list(list_t);
     delete_middle_list(list_s);
 }

 //TODO: final_join_parallel



/* 
 * middle_list.c
 */
struct middle_list_node* create_middle_list_node();
int is_middle_list_bucket_full(middle_list_bucket*);
int append_to_middle_bucket(middle_list_bucket*, uint64_t);
 void testCreate_middle_list_node()
 {
     middle_list_node* list_node=create_middle_list_node();
     //Check if the middle list node is created and initialized correctly
     if(list_node==NULL||list_node->next!=NULL||list_node->bucket.index_to_add_next!=0)
     {
         CU_ASSERT(0);
         return;
     }
     //Check if the bucket size is correct
     if(sizeof(list_node->bucket.row_ids)!=middle_LIST_BUCKET_SIZE*sizeof(uint64_t))
     {
         CU_ASSERT(0);
     }
     free(list_node);
 }

 void testIs_middle_list_bucket_full()
 {
     //Create a middle bucket
     middle_list_bucket bucket;
     bucket.index_to_add_next=0;
     //Check the index when empty
     if(is_middle_list_bucket_full(&bucket))
     {
         CU_ASSERT(0);
     }
     bucket.index_to_add_next=middle_LIST_BUCKET_SIZE;
     //Check the index when full
     if(!is_middle_list_bucket_full(&bucket))
     {
         CU_ASSERT(0);
     }
 }

 void testAppend_to_middle_bucket()
 {
     //Initialize the testing variables
     middle_list_bucket bucket;
     bucket.index_to_add_next=0;
     uint64_t r_row_id=1;
     //Check the append operation
     unsigned int i=0;
     for(;i<middle_LIST_BUCKET_SIZE;i++)
     {
         if(append_to_middle_bucket(&bucket, r_row_id)!=0||bucket.row_ids[i]!=r_row_id)
         {
             CU_ASSERT(0);
         }
         r_row_id+=2;
     }
     //Test if bucket is full (append to bucket must fail)
     if(append_to_middle_bucket(&bucket, r_row_id)!=1&&bucket.index_to_add_next!=i)
     {
         CU_ASSERT(0);
     }
     //print_middle_bucket(&bucket,stdout);
 }

 void testCreate_middle_list()
 {
     //Create a middle list
     middle_list* list=create_middle_list();
     //Check if the list is initialized correctly
     if(list==NULL||list->head!=NULL||list->number_of_nodes!=0||list->tail!=NULL)
     {
         CU_ASSERT(0);
     }
     free(list);
 }

//TODO: #if defined(SERIAL_EXECUTION)||(defined(SERIAL_JOIN)&&defined(SERIAL_FILTER)&&defined(SERIAL_SELFJOIN))
 void testConstruct_lookup_table()
 {
    lookup_table *lt;

    //Giving NULL as list, should return NULL
    lt = construct_lookup_table(NULL);
    CU_ASSERT_EQUAL(lt, NULL);

    middle_list *list = create_middle_list();
    unsigned int num_buckets = 100;


    //Add num_buckets buckets to the list
    middle_list_node *new = create_middle_list_node();
    new->bucket.row_ids[0] = 0;
    list->head = new;
    middle_list_node *prev = list->head;

    for(unsigned int i=1; i<num_buckets; i++)
    {
        new = create_middle_list_node();
        new->bucket.row_ids[0] = i;
        prev->next = new;
        prev = prev->next;
    }
    list->tail = new;
    list->number_of_nodes = num_buckets;

    lt = construct_lookup_table(list);
    for(unsigned int i=0; i<num_buckets; i++)
    {
        CU_ASSERT_EQUAL(lt->lookup_table[i]->row_ids[0], i);
    }

    CU_ASSERT_EQUAL(lt->size, list->number_of_nodes);

    delete_lookup_table(lt);
    delete_middle_list(list);

 }

 //TODO: else ?

 void testAppend_to_middle_list()
 {
     //Create a middle list
     middle_list* list=create_middle_list();
     if(list==NULL)//Check if it was created
     {
         CU_ASSERT(0);
         return;
     }
     uint64_t r_row_id=1;
     unsigned int i=0;
     //Append the maximum records of one node in the list
     for(;i<middle_LIST_BUCKET_SIZE;i++)
     {
         //Check if the items were added correctly in head and tail nodes
         if(append_to_middle_list(list, r_row_id)!=0||
                 list->head->bucket.row_ids[i]!=r_row_id||
                 list->tail->bucket.row_ids[i]!=r_row_id)
         {
             CU_ASSERT(0);
         }
         r_row_id+=2;
     }
     //Check if only one node exists and it is full
     if(list->number_of_nodes!=1||is_middle_list_bucket_full(&list->head->bucket)!=1||list->head->bucket.index_to_add_next!=i||list->head!=list->tail)
     {
         CU_ASSERT(0);
     }
     //Append the maximum records of one node in the list
     for(i=0;i<middle_LIST_BUCKET_SIZE;i++)
     {
         //Check if the items were added correctly in tail node
         if(append_to_middle_list(list, r_row_id)!=0||list->tail->bucket.row_ids[i]!=r_row_id)
         {
             CU_ASSERT(0);
         }
         r_row_id+=2;
     }
     //Check if two nodes exists and are both full
     if(list->number_of_nodes!=2||is_middle_list_bucket_full(&list->head->bucket)!=1||is_middle_list_bucket_full(&list->tail->bucket)!=1||list->head->bucket.index_to_add_next!=i||list->tail->bucket.index_to_add_next!=i||list->head==list->tail)
     {
         CU_ASSERT(0);
     }
     //print_middle_list(list,stdout);
     delete_middle_list(list);
 }

 void testIs_middle_list_empty()
 {
     middle_list* list=create_middle_list();
     if(list==NULL)
     {
         CU_ASSERT(0);
         return;
     }
     if(is_middle_list_empty(list)!=1)
     {
         CU_ASSERT(0);
     }
     if(append_to_middle_list(list, 1)!=0||is_middle_list_empty(list)!=0)
     {
         CU_ASSERT(0);
     }
     //print_middle_list(list,stdout);
     delete_middle_list(list);
 }

 void testMiddle_list_get_number_of_buckets()
 {
     middle_list* list=create_middle_list();
     if(list==NULL)
     {
         CU_ASSERT(0);
         return;
     }
     uint64_t r_row_id=1;
     int i=0;
     for(;i<middle_LIST_BUCKET_SIZE*6;i++)
     {
         if(append_to_middle_list(list, r_row_id)!=0||
                 list->tail->bucket.row_ids[i%middle_LIST_BUCKET_SIZE]!=r_row_id)
         {
             CU_ASSERT(0);
         }
         r_row_id+=2;
     }
     if(list->number_of_nodes!=6)
     {
         CU_ASSERT(0);
     }
     //print_middle_list(list,stdout);
     delete_middle_list(list);
 }

 void testMiddle_list_get_number_of_records()
 {
     middle_list* list=create_middle_list();
     if(list==NULL)
     {
         CU_ASSERT(0);
         return;
     }
     uint64_t r_row_id=1;
     int i=0;
     for(;i<middle_LIST_BUCKET_SIZE*6;i++)
     {
         if(append_to_middle_list(list, r_row_id)!=0||
                 list->tail->bucket.row_ids[i%middle_LIST_BUCKET_SIZE]!=r_row_id)
         {
             CU_ASSERT(0);
         }
         r_row_id+=2;
     }
     if(middle_list_get_number_of_records(list)!=middle_LIST_BUCKET_SIZE*6)
     {
         CU_ASSERT(0);
     }
     //print_middle_list(list,stdout);
     delete_middle_list(list);
 }


 /*
  * table.c
  */

/**
 * Test case 1: NULL table
 */
 void testTable_from_File1()
 {
 	table *t = NULL;
 	int res = table_from_file(t, "./test_files/t1");
 	printf("res = %d\n", res);
 	CU_ASSERT_NOT_EQUAL(res, 0);
 }

/**
 * Test case 2: File does not exist
 */
 void testTable_from_File2()
 {
 	table *t = malloc(sizeof(table));
 	int res = table_from_file(t, "./test_files/doesnt_exist");
 	printf("res = %d\n", res);
 	CU_ASSERT_NOT_EQUAL(res, 0);
 	free(t);
 }

/**
 * Test case 3: Empty file
 */

 void testTable_from_File3()
 {
 	table *t = malloc(sizeof(table));
 	int res = table_from_file(t, "./test_files/empty_file.txt");
 	printf("res = %d\n", res);
 	CU_ASSERT_NOT_EQUAL(res, 0);
 	free(t);
 }

/**
 * Test case 4: File t1 (1 column, 10 rows)
 */
void testTable_from_File4()
{
	table *t = malloc(sizeof(table));
	int res = table_from_file(t, "./test_files/t1");
	CU_ASSERT_EQUAL(res, 0);

	int columns = 1, rows = 10;
	for(uint64_t i=0; i<columns; i++)
	{
		for(uint64_t j=0; j<rows; j++)
		{
			CU_ASSERT_EQUAL(t->array[i][j], i+j);
		}
	}
        delete_table(t);
}

/**
 * Test case 5: File t2 (10 columns, 1 row)
 */
void testTable_from_File5()
{
	table *t = malloc(sizeof(table));
	int res = table_from_file(t, "./test_files/t2");
	CU_ASSERT_EQUAL(res, 0);

	int columns = 10, rows = 1;
	for(uint64_t i=0; i<columns; i++)
	{
		for(uint64_t j=0; j<rows; j++)
		{
			CU_ASSERT_EQUAL(t->array[i][j], i+j);
		}
	}
        delete_table(t);
}

/**
 * Test case 6: File t3 (1000 columns, 1000 rows)
 */
void testTable_from_File6()
{
	table *t = malloc(sizeof(table));
	int res = table_from_file(t, "./test_files/t3");
	CU_ASSERT_EQUAL(res, 0);

	int columns = 1000, rows = 1000;
	for(uint64_t i=0; i<columns; i++)
	{
		for(uint64_t j=0; j<rows; j++)
		{
			CU_ASSERT_EQUAL(t->array[i][j], i+j);
		}
	}
        delete_table(t);
}

//TODO: insert_tables_from_list

/**
 * Test case 1: Table index is NULL
 */
void testGet_table1()
{
	table_index *ti = NULL;
	table *t = get_table(ti, 1);
	CU_ASSERT_EQUAL(t, NULL);
}

/**
 * Test case 2: Tables of table index is NULL
 */
void testGet_table2()
{
	table_index *ti = malloc(sizeof(table_index));
	ti->tables = NULL;
	table *t = get_table(ti, 1);
	CU_ASSERT_EQUAL(t, NULL);
        free(ti);
}

/**
 * Test case 3: Table does not exist
 */
void testGet_table3()
{
	table_index *ti = malloc(sizeof(table_index));
	ti->num_tables = 5;
	ti->tables = malloc(ti->num_tables*sizeof(table));
	for(uint32_t i=0; i<ti->num_tables; i++)
	{
		ti->tables[i].table_id = i;
	}

	table *t = get_table(ti, 10);
	CU_ASSERT_EQUAL(t, NULL);
        free(ti->tables);
        free(ti);
}

/**
 * Test case 3: Table exists
 */
void testGet_table4()
{
	table_index *ti = malloc(sizeof(table_index));
	ti->num_tables = 100;
	ti->tables = malloc(ti->num_tables*sizeof(table));
	for(uint32_t i=0; i<ti->num_tables; i++)
	{
		ti->tables[i].table_id = i;
	}

	table *t = get_table(ti, 10);
	CU_ASSERT_EQUAL(t->table_id, 10);
        free(ti->tables);
        free(ti);
}

//TODO: insert_tables


/*
 * string_list.c
 */
void testString_list_create()
{
    string_list *list = string_list_create();
    CU_ASSERT_EQUAL(list->num_nodes, 0);
    CU_ASSERT_EQUAL(list->head, NULL);
    CU_ASSERT_EQUAL(list->tail, NULL);
    free(list);
}

/**
 * Test case 1: list is NULL
 */
void testString_list_insert1()
{
    string_list *list = NULL;
    int res = string_list_insert(list, "string1");
    CU_ASSERT_NOT_EQUAL(res, 0);
}

/**
 * Test case 2: insert 3 strings
 */
void testString_list_insert2()
{
    string_list *list = string_list_create();
    int n = 3;
    for(int i=0; i<n; i++)
    {
        char str[20];
        sprintf(str, "string%d", i);
        int res = string_list_insert(list, str);
        CU_ASSERT_EQUAL(res, 0);
    }

    string_list_node *node = list->head;
    for(int i=0; i<n; i++)
    {
        char str[20];
        sprintf(str, "string%d", i);
        CU_ASSERT_EQUAL(strcmp(node->string, str), 0);
        node = node->next;
    }

    node = list->head;
    while(node != NULL)
    {
        string_list_node *temp = node;
        node = node->next;
        free(temp);
    }

    free(list);
}

/**
 * Test case 3: insert 100 strings
 */
void testString_list_insert3()
{
    string_list *list = string_list_create();
    int n = 100;
    for(int i=0; i<n; i++)
    {
        char str[20];
        sprintf(str, "string%d", i);
        int res = string_list_insert(list, str);
        CU_ASSERT_EQUAL(res, 0);
    }

    string_list_node *node = list->head;
    for(int i=0; i<n; i++)
    {
        char str[20];
        sprintf(str, "string%d", i);
        CU_ASSERT_EQUAL(strcmp(node->string, str), 0);
        node = node->next;
    }

    node = list->head;
    while(node != NULL)
    {
        string_list_node *temp = node;
        node = node->next;
        free(temp);
    }

    free(list);
}

/**
 * Test case 1: list is NULL
 */
void testString_list_remove1()
{
    string_list *list = NULL;
    char *str = string_list_remove(list);
    CU_ASSERT_EQUAL(str, NULL);
}

/**
 * Test case 2: list is empty
 */
void testString_list_remove2()
{
    string_list *list = string_list_create();
    char *str = string_list_remove(list);
    CU_ASSERT_EQUAL(str, NULL);

    free(list);
}

/**
 * Test case 3: remove 3 of 10 strings
 */
void testString_list_remove3()
{
    string_list *list = string_list_create();
    int n = 10;
    for(int i=0; i<n; i++)
    {
        char str[20];
        sprintf(str, "string%d", i);
        string_list_insert(list, str);
    }

    for(int i=0; i<3; i++)
    {
        char str[20];
        sprintf(str, "string%d", i);
        char *str2 = string_list_remove(list);
        CU_ASSERT_EQUAL(strcmp(str, str2), 0);
        free(str2);
    }

    string_list_node *node = list->head;
    while(node != NULL)
    {
        string_list_node *temp = node;
        node = node->next;
        free(temp);
    }

    free(list);
}

/**
 * Test case 4: remove 100 of 100 strings
 */
void testString_list_remove4()
{
    string_list *list = string_list_create();
    int n = 100;
    for(int i=0; i<n; i++)
    {
        char str[20];
        sprintf(str, "string%d", i);
        string_list_insert(list, str);
    }

    for(int i=0; i<n; i++)
    {
        char str[20];
        sprintf(str, "string%d", i);
        char *str2 = string_list_remove(list);
        CU_ASSERT_EQUAL(strcmp(str, str2), 0);
        free(str2);
    }

    free(list);
}


/*
 * execute_query.c
 */

/**
 * Test case 1: 0 number_of_tables given (should return NULL)
 */
void testInitialize_middleman1()
{
    middleman *m = initialize_middleman(0);
    CU_ASSERT_EQUAL(m, NULL);
}

/**
 * Test case 2: 1000 number_of_tables given
 */
void testInitialize_middleman2()
{
    uint32_t num = 1000;
    middleman *m = initialize_middleman(num);
    CU_ASSERT_NOT_EQUAL_FATAL(m, NULL);
    CU_ASSERT_EQUAL(m->number_of_tables, num);
    CU_ASSERT_NOT_EQUAL_FATAL(m->tables, NULL);

    for(uint32_t i=0; i<num; i++)
    {
        CU_ASSERT_EQUAL(m->tables[i].list, NULL);
    }

    free(m->tables);
    free(m);
}

/**
 * Test case 1: table NULL
 */
void testConstruct_relation_from_table1()
{
    relation* rel = construct_relation_from_table(NULL, 1);
    CU_ASSERT_EQUAL(rel, NULL);
}

/**
 * Test case 2: column_id doesn't exist
 */
void testConstruct_relation_from_table2()
{
	table *t = malloc(sizeof(table));
	t->rows = 10;
	t->columns = 2;
	t->array = malloc(t->columns * sizeof(uint64_t *));
    for(uint64_t i=0; i<t->columns; i++)
    {
    	t->array[i] = malloc(t->rows*sizeof(uint64_t));
    }

    relation *rel = construct_relation_from_table(t, 10);
    CU_ASSERT_EQUAL(rel, NULL);
    for(uint64_t i=0; i<t->columns; i++)
    {
    	free(t->array[i]);
    }
    free(t->array);
    free(t);
}


/**
 * Test case 3: Small table 3x4
 */
void testConstruct_relation_from_table3()
{
    table *t = malloc(sizeof(table));
	t->rows = 4;
	t->columns = 3;
	t->array = malloc(t->columns * sizeof(uint64_t *));
    for(uint64_t i=0; i<t->columns; i++)
    {
    	t->array[i] = malloc(t->rows*sizeof(uint64_t));
    }

    for(uint64_t i=0; i<t->columns; i++)
    {
    	for(uint64_t j=0; j<t->rows; j++)
    	{
    		t->array[i][j] = i + j;
    	}
    }

    relation *rel = construct_relation_from_table(t, 1);
    CU_ASSERT_NOT_EQUAL(rel, NULL);
    CU_ASSERT_EQUAL(rel->num_tuples, t->rows);
    for(uint64_t j=0; j<t->rows; j++)
    {
    	CU_ASSERT_EQUAL(rel->tuples[j].key, 1+j);
    	CU_ASSERT_EQUAL(rel->tuples[j].row_id, j);
    }

    for(uint64_t i=0; i<t->columns; i++)
    {
    	free(t->array[i]);
    }
    free(t->array);
    free(t);

    free(rel->tuples);
    free(rel);
}


/**
 * Test case 4: Big table 1000x1000
 */
void testConstruct_relation_from_table4()
{
    table *t = malloc(sizeof(table));
	t->rows = 1000;
	t->columns = 1000;
	t->array = malloc(t->columns * sizeof(uint64_t *));
    for(uint64_t i=0; i<t->columns; i++)
    {
    	t->array[i] = malloc(t->rows*sizeof(uint64_t));
    }

    for(uint64_t i=0; i<t->columns; i++)
    {
    	for(uint64_t j=0; j<t->rows; j++)
    	{
    		t->array[i][j] = i + j;
    	}
    }

    relation *rel = construct_relation_from_table(t, 5);
    CU_ASSERT_NOT_EQUAL(rel, NULL);
    CU_ASSERT_EQUAL(rel->num_tuples, t->rows);
    for(uint64_t j=0; j<t->rows; j++)
    {
    	CU_ASSERT_EQUAL(rel->tuples[j].key, 5+j);
    	CU_ASSERT_EQUAL(rel->tuples[j].row_id, j);
    }

    for(uint64_t i=0; i<t->columns; i++)
    {
    	free(t->array[i]);
    }
    free(t->array);
    free(t);

    free(rel->tuples);
    free(rel);
}


/**
 * Test case 1: called once for one bucket
 */
void testConstruct_relation_from_middleman1()
{
	uint64_t items = 10;

    //Initialize table with data
    table *t = malloc(sizeof(table));
    t->rows = items;
    t->columns = 3;
    t->array = malloc(t->columns * sizeof(uint64_t *));
    for(uint64_t i=0; i<t->columns; i++)
    {
        t->array[i] = malloc(t->rows*sizeof(uint64_t));
    }

    for(uint64_t i=0; i<t->columns; i++)
    {
        for(uint64_t j=0; j<t->rows; j++)
        {
            t->array[i][j] = i + j;
        }
    }


	//Initialize middle_list_bucket with data
	middle_list_bucket *bucket = malloc(sizeof(middle_list_bucket));
	for(uint64_t i=0; i<items; i++)
	{
		bucket->row_ids[i] = i;
	}
    bucket->index_to_add_next = items;

	//Initialize relation to receive data
    relation *rel = malloc(sizeof(relation));
	rel->num_tuples = items;
	rel->tuples = malloc(items*sizeof(tuple));

	uint64_t column = 1;
	uint64_t counter = 0;

	construct_relation_from_middleman(bucket, t, rel, column, &counter);
	CU_ASSERT_EQUAL(counter, items);

	for(uint64_t i=0; i<items; i++)
	{
		CU_ASSERT_EQUAL(rel->tuples[i].key, t->array[column][i]);
		CU_ASSERT_EQUAL(rel->tuples[i].row_id, i);
	}

	//Free table
	for(uint64_t i=0; i<t->columns; i++)
	{
		free(t->array[i]);
	}
	free(t->array);
	free(t);

	//Free relation
	free(rel->tuples);
	free(rel);

	//Free middle_list_bucket
	free(bucket);
}


/**
 * Test case 2: called multiple times for different buckets
 */
void testConstruct_relation_from_middleman2()
{
	uint64_t items = 20;

	//Initialize table with data
	table *t = malloc(sizeof(table));
	t->rows = items;
	t->columns = 3;
	t->array = malloc(t->columns * sizeof(uint64_t *));
    for(uint64_t i=0; i<t->columns; i++)
    {
    	t->array[i] = malloc(t->rows*sizeof(uint64_t));
    }

    for(uint64_t i=0; i<t->columns; i++)
    {
    	for(uint64_t j=0; j<t->rows; j++)
    	{
    		t->array[i][j] = i + j;
    	}
    }


    uint64_t r = 0;
	//Initialize middle_list_bucket with data
	middle_list_bucket *bucket1 = malloc(sizeof(middle_list_bucket));
	for(uint64_t i=0; i<items/2; i++)
	{
		bucket1->row_ids[i] = r;
        r++;
	}
    bucket1->index_to_add_next = items/2;
	//Initialize middle_list_bucket with data
	middle_list_bucket *bucket2 = malloc(sizeof(middle_list_bucket));
	for(uint64_t i=0; i<items; i++)
	{
		bucket2->row_ids[i] = r;
        r++;
	}
    bucket2->index_to_add_next = items/2;


	//Initialize relation to receive data
    relation *rel = malloc(sizeof(relation));
	rel->num_tuples = items;
	rel->tuples = malloc(items*sizeof(tuple));

	uint64_t column = 1;
	uint64_t counter = 0;

	construct_relation_from_middleman(bucket1, t, rel, column, &counter);
	CU_ASSERT_EQUAL(counter, items/2);


	construct_relation_from_middleman(bucket2, t, rel, column, &counter);
    CU_ASSERT_EQUAL(counter, items);


	r = 0;
	for(; r<items; r++)
	{
		CU_ASSERT_EQUAL(rel->tuples[r].key, t->array[column][r]);
		CU_ASSERT_EQUAL(rel->tuples[r].row_id, r);
	}

	//Free table
	for(uint64_t i=0; i<t->columns; i++)
	{
		free(t->array[i]);
	}
	free(t->array);
	free(t);


	//Free relation
	free(rel->tuples);
	free(rel);


	//Free middle_list_bucket
	free(bucket1);
    free(bucket2);

}

//TODO: update_related_lists #ifdef
//TODO: #ifdef update_middle_bucket
//TODO: #else update_middle_bucket
//TODO: self_join_middle_bucket
//TODO: self_join_middle_bucket_parallel
//TODO: filter_original_table_parallel
//TODO: self_join_table_parallel
//TODO: execute_query_parallel #ifdef
//TODO: calculate_sum


/*
 * query.c
 */

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

//TODO: create_query
//TODO: parse_predicate

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

//TODO: compare_predicates
//TODO: swap_predicates
//TODO: create_sort_array

void move_predicate(query* q, uint32_t index_start, uint32_t index_end);

void testMove_predicate(void)
{
    query *q=create_query();
    query *q1=create_query();
    query *q2=create_query();
    query *q3=create_query();
    query *q4=create_query();
    if(q==NULL||q1==NULL||q2==NULL||q3==NULL||q4==NULL)
    {
        return;
    }
    char query_str[80];
    char query_str_1[80];
    char query_str_2[80];
    char query_str_3[80];
    char query_str_4[80];
    strncpy(query_str, "3 0 2 1|0.2=1.0&1.0=2.1&2.1=3.2&0.2<74|1.2 2.5 3.5", 79);
    strncpy(query_str_1, "3 0 2 1|1.0=2.1&0.2=1.0&2.1=3.2&0.2<74|1.2 2.5 3.5", 79);
    strncpy(query_str_2, "3 0 2 1|0.2<74&1.0=2.1&0.2=1.0&2.1=3.2|1.2 2.5 3.5", 79);
    strncpy(query_str_3, "3 0 2 1|0.2<74&0.2=1.0&2.1=3.2&1.0=2.1|1.2 2.5 3.5", 79);
    strncpy(query_str_4, "3 0 2 1|2.1=3.2&0.2<74&0.2=1.0&1.0=2.1|1.2 2.5 3.5", 79);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str, q), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_1, q1), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_2, q2), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_3, q3), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_4, q4), 0);
    move_predicate(q,0,1);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q, q1), true);
    move_predicate(q,3,0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q, q2), true);
    move_predicate(q,1,3);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q, q3), true);
    move_predicate(q,2,0);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q, q4), true);
    delete_query(q);
    delete_query(q1);
    delete_query(q2);
    delete_query(q3);
    delete_query(q4);
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
    ti.tables=NULL;
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
typedef struct counter_list counter_list;
counter_list* create_counter_list(void);
int counter_list_append(counter_list* list, table_column* tc);
int counter_list_remove(counter_list* list, table_column* tc);
uint32_t counter_list_get_counter(counter_list* list, table_column* tc);
void delete_counter_list(counter_list* list);
void test_counter_list(void)
{
    counter_list* list=create_counter_list();
    CU_ASSERT_NOT_EQUAL_FATAL(list,NULL);
    table_column tc1_1;//one time
    table_column tc2_2;//two times
    table_column tc3_2;//two times
    table_column tc4_3;//tree times
    table_column tc5_4;//four times
    tc1_1.table_id=1;
    tc1_1.column_id=1;
    tc2_2.table_id=2;
    tc2_2.column_id=2;
    tc3_2.table_id=3;
    tc3_2.column_id=3;
    tc4_3.table_id=2;
    tc4_3.column_id=1;
    tc5_4.table_id=1;
    tc5_4.column_id=3;
    CU_ASSERT_EQUAL_FATAL(counter_list_append(list,&tc1_1),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_get_counter(list,&tc1_1),1);
    CU_ASSERT_EQUAL_FATAL(counter_list_append(list,&tc2_2),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_append(list,&tc2_2),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_get_counter(list,&tc2_2),2);
    CU_ASSERT_EQUAL_FATAL(counter_list_append(list,&tc3_2),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_append(list,&tc3_2),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_get_counter(list,&tc3_2),2);
    CU_ASSERT_EQUAL_FATAL(counter_list_append(list,&tc4_3),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_append(list,&tc4_3),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_append(list,&tc4_3),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_get_counter(list,&tc4_3),3);
    CU_ASSERT_EQUAL_FATAL(counter_list_append(list,&tc5_4),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_append(list,&tc5_4),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_append(list,&tc5_4),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_append(list,&tc5_4),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_get_counter(list,&tc5_4),4);
    CU_ASSERT_EQUAL_FATAL(counter_list_remove(list,&tc1_1),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_get_counter(list,&tc1_1),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_remove(list,&tc5_4),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_get_counter(list,&tc5_4),3);
    CU_ASSERT_EQUAL_FATAL(counter_list_remove(list,&tc2_2),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_get_counter(list,&tc2_2),1);
    CU_ASSERT_EQUAL_FATAL(counter_list_remove(list,&tc2_2),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_get_counter(list,&tc2_2),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_remove(list,&tc5_4),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_get_counter(list,&tc5_4),2);
    CU_ASSERT_EQUAL_FATAL(counter_list_remove(list,&tc3_2),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_get_counter(list,&tc3_2),1);
    CU_ASSERT_EQUAL_FATAL(counter_list_remove(list,&tc3_2),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_get_counter(list,&tc3_2),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_remove(list,&tc5_4),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_get_counter(list,&tc5_4),1);
    CU_ASSERT_EQUAL_FATAL(counter_list_remove(list,&tc4_3),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_get_counter(list,&tc4_3),2);
    CU_ASSERT_EQUAL_FATAL(counter_list_remove(list,&tc5_4),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_get_counter(list,&tc5_4),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_remove(list,&tc4_3),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_get_counter(list,&tc4_3),1);
    CU_ASSERT_EQUAL_FATAL(counter_list_remove(list,&tc4_3),0);
    CU_ASSERT_EQUAL_FATAL(counter_list_get_counter(list,&tc4_3),0);
    
    delete_counter_list(list);
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

   	if(table_from_file(&(ti.tables[0]), "test_files/r0") != 0)
   	{
   		fprintf(stderr, "testOptimize_query1: Error in table_from_file\n");
   		return;
   	}
   	if(table_from_file(&(ti.tables[1]), "test_files/r1") != 0)
   	{
   		fprintf(stderr, "testOptimize_query1: Error in table_from_file\n");
   		return;
   	}
   	if(table_from_file(&(ti.tables[2]), "test_files/r2") != 0)
   	{
   		fprintf(stderr, "testOptimize_query1: Error in table_from_file\n");
   		return;
   	}
   	if(table_from_file(&(ti.tables[3]), "test_files/r3") != 0)
   	{
   		fprintf(stderr, "testOptimize_query1: Error in table_from_file\n");
   		return;
   	}
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
    strncpy(query_str2, "1 0|0.2=1.0&0.2=9881|1.1 0.2 1.0", 79);
    strncpy(query_str_c_2, "1 0|0.2=9881&0.2=1.0|1.1 0.2 1.0", 79);
    bool q_b_2[]={true, true};
    strncpy(query_str3, "3 0 2|0.1=1.0&1.0=2.2&0.0>12472|1.0 0.3", 79);
    strncpy(query_str_c_3, "3 0 2|0.0>12472&1.0=2.2&0.1=1.0|1.0 0.3", 79);
    bool q_b_3[]={true, true, true, false};
    strncpy(query_str4, "3 0|0.1=1.0&0.1>1150|0.3 1.0", 79);
    strncpy(query_str_c_4, "3 0|0.1>1150&0.1=1.0|0.3 1.0", 79);
    bool q_b_4[]={true, true};
    strncpy(query_str5, "2 1 3|0.1=1.0&1.0=2.2&0.0<62236|1.0", 79);
    strncpy(query_str_c_5, "2 1 3|0.0<62236&1.0=2.2&0.1=1.0|1.0", 79);
    bool q_b_5[]={true, true, true, false};
/*    strncpy(query_str6, "3 0 2|0.2=1.0&1.0=2.2&0.1=5784|2.1 0.1 0.1", 79);
    strncpy(query_str_c_6, "3 0 2|0.1=5784&1.0=2.2&0.2=1.0|2.1 0.1 0.1", 79);
    bool q_b_6[]={true, true, true, false};
    strncpy(query_str7, "0 1 2 3|0.1=1.0&1.0=2.1&1.0=3.1&0.1>2493|3.2 2.2 2.1", 79);
    strncpy(query_str_c_7, "0 1 2 3|0.1>2493&1.0=2.1&1.0=3.1&0.1=1.0|3.2 2.2 2.1", 79);
    bool q_b_7[]={true, true, false, true, true, false};
    strncpy(query_str8, "2 0 3 1|0.2=1.0&1.0=2.2&0.1=3.0&0.1=209|0.2 2.1 2.2", 79);
    strncpy(query_str_c_8, "2 0 3 1|0.1=209&1.0=2.2&0.2=1.0&0.1=3.0|0.2 2.1 2.2", 79);
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
    strncpy(query_str12, "2 0 3 1|0.2=1.0&1.0=2.2&1.0=3.2&0.0<9872|3.0 2.2", 79);
    strncpy(query_str_c_12, "2 0 3 1|0.0<9872&1.0=2.2&1.0=3.2&0.2=1.0|3.0 2.2", 79);
    bool q_b_12[]={true, true, false, true, true, false};
    strncpy(query_str13, "2 0 1 1|0.2=1.0&1.0=2.2&2.1=3.2&0.1>7860|3.2 2.1", 79);
    strncpy(query_str_c_13, "2 0 1 1|0.1>7860&1.0=2.2&0.2=1.0&2.1=3.2|3.2 2.1", 79);
    bool q_b_13[]={true, true, true, false, true, true};
    strncpy(query_str14, "1 0 2 3|0.0=1.1&0.0=2.2&0.0=3.1&1.1>2936|1.0 1.0 3.0", 79);
    strncpy(query_str_c_14, "1 0 2 3|1.1>2936&0.0=2.2&0.0=3.1&0.0=1.1|1.0 1.0 3.0", 79);
    bool q_b_14[]={true, true, false, true, false, true};
    strncpy(query_str15, "1 0 2|0.1=1.0&1.0=0.1&1.0=2.1&0.1>3791|1.2 1.2", 79);
    strncpy(query_str_c_15, "1 0 2|0.1>3791&1.0=0.1&1.0=2.1|1.2 1.2", 79);
    bool q_b_15[]={true, true, false, true};
    strncpy(query_str16, "3 0 2|0.2=1.0&1.0=0.1&1.0=2.2&0.1>4477|2.0 2.1 1.2", 79);
    strncpy(query_str_c_16, "3 0 2|0.1>4477&1.0=0.1&1.0=2.2&0.2=1.0|2.0 2.1 1.2", 79);
    bool q_b_16[]={true, true, false, true};
    strncpy(query_str17, "1 0 2|0.0=1.2&0.0=2.1&1.1=0.0&1.0>25064|0.2 1.0", 79);
    strncpy(query_str_c_17, "1 0 2|1.0>25064&0.0=2.1&0.0=1.1&0.0=1.2|0.2 1.0", 79);
    bool q_b_17[]={true, true, false, true};
    strncpy(query_str18, "3 1 0|0.2=1.0&1.0=2.1&1.0=0.2&0.3>3991|1.0", 79);
    strncpy(query_str_c_18, "3 1 0|0.3>3991&1.0=2.1&0.2=1.0|1.0", 79);
    bool q_b_18[]={true, true, true, false};
    strncpy(query_str19, "3 0 2 2|0.2=1.0&1.0=2.1&2.1=3.2&0.2<74|1.2 2.0 3.0", 79);
    strncpy(query_str_c_19, "3 0 2 2|0.2<74&1.0=0.2&2.1=1.0&2.1=3.2|1.2 2.0 3.0", 79);
    bool q_b_19[]={true, true, true, false, false, true};
    strncpy(query_str20, "0 2 1 3|0.0=1.1&0.0=2.2&0.0=3.2&1.2=8728|2.0 3.1", 79);
    strncpy(query_str_c_20, "0 2 1 3|1.2=8728&0.0=2.2&0.0=3.2&0.0=1.1|2.0 3.1", 79);
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
    strncpy(query_str26, "2 0 1|0.2=1.0&1.0=2.2&0.2<9473|0.2 2.0", 79);
    strncpy(query_str_c_26, "2 0 1|0.2<9473&1.0=2.2&0.2=1.0|0.2 2.0", 79);
    bool q_b_26[]={true, true, true, false};
    strncpy(query_str27, "0 1 3|0.2=1.0&1.0=2.1&1.0=0.2&0.2>6082|2.3 2.1", 79);
    strncpy(query_str_c_27, "0 1 3|0.2>6082&1.0=2.1&0.2=1.0|2.3 2.1", 79);
    bool q_b_27[]={true, true, true, false};
    strncpy(query_str28, "1 0 3|0.1=1.0&1.0=2.2&0.0=10571|2.3 0.0", 79);
    strncpy(query_str_c_28, "1 0 3|0.0=10571&1.0=2.2&0.1=1.0|2.3 0.0", 79);
    bool q_b_28[]={true, true, true, false};
    strncpy(query_str29, "3 1 2 0|0.1=1.0&1.0=2.1&1.0=3.1&0.2=598|3.2", 79);
    strncpy(query_str_c_29, "3 1 2 0|0.2=598&1.0=2.1&1.0=3.1&0.1=1.0|3.2", 79);
    bool q_b_29[]={true, true, false, true, true, false};
    strncpy(query_str30, "0 1 2|0.2=1.0&1.0=2.1&2.2=1.0&0.2<2685|2.0", 79);
    strncpy(query_str_c_30, "0 1 2|0.2<2685&1.0=2.1&1.0=2.2&0.2=1.0|2.0", 79);
    bool q_b_30[]={true, true, true, false};
    strncpy(query_str31, "2 0 1|0.2=1.0&1.0=2.1&0.1>10502|1.1 1.2 2.0", 79);
    strncpy(query_str_c_31, "2 0 1|0.1>10502&1.0=2.1&0.2=1.0|1.1 1.2 2.0", 79);
    bool q_b_31[]={true, true, true, false};
    strncpy(query_str32, "2 0 1|0.2=1.0&1.0=2.2&0.1<5283|0.0 0.2 2.1", 79);
    strncpy(query_str_c_32, "2 0 1|0.1<5283&1.0=2.2&0.2=1.0|0.0 0.2 2.1", 79);
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
    strncpy(query_str36, "0 1 3|0.2=1.0&1.0=2.1&0.2<8722|1.0", 79);
    strncpy(query_str_c_36, "0 1 3|0.2<8722&1.0=2.1&0.2=1.0|1.0", 79);
    bool q_b_36[]={true, true, true, false};
    strncpy(query_str37, "2 1|0.1=1.0&0.1<9795|1.2 0.1", 79);
    strncpy(query_str_c_37, "2 1|0.1<9795&0.1=1.0|1.2 0.1", 79);
    bool q_b_37[]={true, true};
    strncpy(query_str38, "2 0 1|0.2=1.0&1.0=2.2&0.0=9477|0.2", 79);
    strncpy(query_str_c_38, "2 0 1|0.0=9477&1.0=2.2&0.2=1.0|0.2", 79);
    bool q_b_38[]={true, true, true, false};
    strncpy(query_str39, "0 1 2|0.1=1.0&1.0=2.1&0.1<3560|1.2", 79);
    strncpy(query_str_c_39, "0 1 2|0.1<3560&1.0=2.1&0.1=1.0|1.2", 79);
    bool q_b_39[]={true, true, true, false};
    strncpy(query_str40, "2 0 1 3|0.1=1.0&1.0=2.1&1.0=3.1&0.1=141152&2.2=10242743|0.1 2.2 2.1", 79);
    strncpy(query_str_c_40, "2 0 1 3|0.1=141152&2.2=10242743&1.0=3.1&1.0=0.1&1.0=2.1|0.1 2.2 2.1", 79);
    bool q_b_40[]={true, true, false, true, false, true};*/

    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str1, q1), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str2, q2), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str3, q3), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str4, q4), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str5, q5), 0);
/*    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str6, q6), 0);
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
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str40, q40), 0);*/
    CU_ASSERT_EQUAL_FATAL(validate_query(q1, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q2, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q3, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q4, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q5, &ti), 0);
/*    CU_ASSERT_EQUAL_FATAL(validate_query(q6, &ti), 0);
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
    CU_ASSERT_EQUAL_FATAL(optimize_query(q40, &ti), 0);*/
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_1, q_c_1), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_1, &ti), 0);
//    CU_ASSERT_EQUAL_FATAL(compare_queries(q1, q_c_1), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_2, q_c_2), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_2, &ti), 0);
//    CU_ASSERT_EQUAL_FATAL(compare_queries(q2, q_c_2), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_3, q_c_3), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_3, &ti), 0);
//    CU_ASSERT_EQUAL_FATAL(compare_queries(q3, q_c_3), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_4, q_c_4), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_4, &ti), 0);
//    CU_ASSERT_EQUAL_FATAL(compare_queries(q4, q_c_4), true);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_5, q_c_5), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_5, &ti), 0);
//    CU_ASSERT_EQUAL_FATAL(compare_queries(q5, q_c_5), true);
/*    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_6, q_c_6), 0);
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
    CU_ASSERT_EQUAL_FATAL(compare_queries(q40, q_c_40), true);*/
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
//    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_3, 4), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q4, &array), 0);
    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_4, 2), true);
    free(array);
    array=NULL;
    CU_ASSERT_EQUAL_FATAL(create_sort_array(q5, &array), 0);
//    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_5, 4), true);
    free(array);
/*    array=NULL;
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
    array=NULL;*/
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
        perror("testOptimize_query_memory1: malloc error");
        return;
    }

   	if(table_from_file(&(ti.tables[0]), "test_files/r0") != 0)
   	{
   		fprintf(stderr, "testOptimize_query_memory1: Error in table_from_file\n");
   		return;
   	}
   	if(table_from_file(&(ti.tables[1]), "test_files/r1") != 0)
   	{
   		fprintf(stderr, "testOptimize_query_memory1: Error in table_from_file\n");
   		return;
   	}
   	if(table_from_file(&(ti.tables[2]), "test_files/r2") != 0)
   	{
   		fprintf(stderr, "testOptimize_query_memory1: Error in table_from_file\n");
   		return;
   	}
   	if(table_from_file(&(ti.tables[3]), "test_files/r3") != 0)
   	{
   		fprintf(stderr, "testOptimize_query_memory1: Error in table_from_file\n");
   		return;
   	}

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
    strncpy(query_str2, "1 0|0.2=1.0&0.2=9881|1.1 0.2 1.0", 79);
    strncpy(query_str_c_2, "1 0|0.2=9881&0.2=1.0|1.1 0.2 1.0", 79);
    bool q_b_2[]={true, true};
    strncpy(query_str3, "3 0 2|0.1=1.0&1.0=2.2&0.0>12472|1.0 0.3", 79);
    strncpy(query_str_c_3, "3 0 2|0.0>12472&1.0=2.2&0.1=1.0|1.0 0.3", 79);
    bool q_b_3[]={true, true, true, false};
    strncpy(query_str4, "3 0|0.1=1.0&0.1>1150|0.3 1.0", 79);
    strncpy(query_str_c_4, "3 0|0.1>1150&0.1=1.0|0.3 1.0", 79);
    bool q_b_4[]={true, true};
    strncpy(query_str5, "2 1 3|0.1=1.0&1.0=2.2&0.0<62236|1.0", 79);
    strncpy(query_str_c_5, "2 1 3|0.0<62236&1.0=2.2&0.1=1.0|1.0", 79);
    bool q_b_5[]={true, true, true, false};
/*    strncpy(query_str6, "3 0 2|0.2=1.0&1.0=2.2&0.1=5784|2.1 0.1 0.1", 79);
    strncpy(query_str_c_6, "3 0 2|0.1=5784&1.0=2.2&0.2=1.0|2.1 0.1 0.1", 79);
    bool q_b_6[]={true, true, true, false};
    strncpy(query_str7, "0 1 2 3|0.1=1.0&1.0=2.1&1.0=3.1&0.1>2493|3.2 2.2 2.1", 79);
    strncpy(query_str_c_7, "0 1 2 3|0.1>2493&1.0=2.1&1.0=3.1&0.1=1.0|3.2 2.2 2.1", 79);
    bool q_b_7[]={true, true, false, true, true, false};
    strncpy(query_str8, "2 0 3 1|0.2=1.0&1.0=2.2&0.1=3.0&0.1=209|0.2 2.1 2.2", 79);
    strncpy(query_str_c_8, "2 0 3 1|0.1=209&1.0=2.2&0.2=1.0&0.1=3.0|0.2 2.1 2.2", 79);
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
    strncpy(query_str12, "2 0 3 1|0.2=1.0&1.0=2.2&1.0=3.2&0.0<9872|3.0 2.2", 79);
    strncpy(query_str_c_12, "2 0 3 1|0.0<9872&1.0=2.2&1.0=3.2&0.2=1.0|3.0 2.2", 79);
    bool q_b_12[]={true, true, false, true, true, false};
    strncpy(query_str13, "2 0 1 1|0.2=1.0&1.0=2.2&2.1=3.2&0.1>7860|3.2 2.1", 79);
    strncpy(query_str_c_13, "2 0 1 1|0.1>7860&1.0=2.2&0.2=1.0&2.1=3.2|3.2 2.1", 79);
    bool q_b_13[]={true, true, true, false, true, true};
    strncpy(query_str14, "1 0 2 3|0.0=1.1&0.0=2.2&0.0=3.1&1.1>2936|1.0 1.0 3.0", 79);
    strncpy(query_str_c_14, "1 0 2 3|1.1>2936&0.0=2.2&0.0=3.1&0.0=1.1|1.0 1.0 3.0", 79);
    bool q_b_14[]={true, true, false, true, false, true};
    strncpy(query_str15, "1 0 2|0.1=1.0&1.0=0.1&1.0=2.1&0.1>3791|1.2 1.2", 79);
    strncpy(query_str_c_15, "1 0 2|0.1>3791&1.0=0.1&1.0=2.1|1.2 1.2", 79);
    bool q_b_15[]={true, true, false, true};
    strncpy(query_str16, "3 0 2|0.2=1.0&1.0=0.1&1.0=2.2&0.1>4477|2.0 2.1 1.2", 79);
    strncpy(query_str_c_16, "3 0 2|0.1>4477&1.0=0.1&1.0=2.2&0.2=1.0|2.0 2.1 1.2", 79);
    bool q_b_16[]={true, true, false, true};
    strncpy(query_str17, "1 0 2|0.0=1.2&0.0=2.1&1.1=0.0&1.0>25064|0.2 1.0", 79);
    strncpy(query_str_c_17, "1 0 2|1.0>25064&0.0=2.1&0.0=1.1&0.0=1.2|0.2 1.0", 79);
    bool q_b_17[]={true, true, false, true};
    strncpy(query_str18, "3 1 0|0.2=1.0&1.0=2.1&1.0=0.2&0.3>3991|1.0", 79);
    strncpy(query_str_c_18, "3 1 0|0.3>3991&1.0=2.1&0.2=1.0|1.0", 79);
    bool q_b_18[]={true, true, true, false};
    strncpy(query_str19, "3 0 2 2|0.2=1.0&1.0=2.1&2.1=3.2&0.2<74|1.2 2.0 3.0", 79);
    strncpy(query_str_c_19, "3 0 2 2|0.2<74&1.0=0.2&2.1=1.0&2.1=3.2|1.2 2.0 3.0", 79);
    bool q_b_19[]={true, true, true, false, false, true};
    strncpy(query_str20, "0 2 1 3|0.0=1.1&0.0=2.2&0.0=3.2&1.2=8728|2.0 3.1", 79);
    strncpy(query_str_c_20, "0 2 1 3|1.2=8728&0.0=2.2&0.0=3.2&0.0=1.1|2.0 3.1", 79);
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
    strncpy(query_str26, "2 0 1|0.2=1.0&1.0=2.2&0.2<9473|0.2 2.0", 79);
    strncpy(query_str_c_26, "2 0 1|0.2<9473&1.0=2.2&0.2=1.0|0.2 2.0", 79);
    bool q_b_26[]={true, true, true, false};
    strncpy(query_str27, "0 1 3|0.2=1.0&1.0=2.1&1.0=0.2&0.2>6082|2.3 2.1", 79);
    strncpy(query_str_c_27, "0 1 3|0.2>6082&1.0=2.1&0.2=1.0|2.3 2.1", 79);
    bool q_b_27[]={true, true, true, false};
    strncpy(query_str28, "1 0 3|0.1=1.0&1.0=2.2&0.0=10571|2.3 0.0", 79);
    strncpy(query_str_c_28, "1 0 3|0.0=10571&1.0=2.2&0.1=1.0|2.3 0.0", 79);
    bool q_b_28[]={true, true, true, false};
    strncpy(query_str29, "3 1 2 0|0.1=1.0&1.0=2.1&1.0=3.1&0.2=598|3.2", 79);
    strncpy(query_str_c_29, "3 1 2 0|0.2=598&1.0=2.1&1.0=3.1&0.1=1.0|3.2", 79);
    bool q_b_29[]={true, true, false, true, true, false};
    strncpy(query_str30, "0 1 2|0.2=1.0&1.0=2.1&2.2=1.0&0.2<2685|2.0", 79);
    strncpy(query_str_c_30, "0 1 2|0.2<2685&1.0=2.1&1.0=2.2&0.2=1.0|2.0", 79);
    bool q_b_30[]={true, true, true, false};
    strncpy(query_str31, "2 0 1|0.2=1.0&1.0=2.1&0.1>10502|1.1 1.2 2.0", 79);
    strncpy(query_str_c_31, "2 0 1|0.1>10502&1.0=2.1&0.2=1.0|1.1 1.2 2.0", 79);
    bool q_b_31[]={true, true, true, false};
    strncpy(query_str32, "2 0 1|0.2=1.0&1.0=2.2&0.1<5283|0.0 0.2 2.1", 79);
    strncpy(query_str_c_32, "2 0 1|0.1<5283&1.0=2.2&0.2=1.0|0.0 0.2 2.1", 79);
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
    strncpy(query_str36, "0 1 3|0.2=1.0&1.0=2.1&0.2<8722|1.0", 79);
    strncpy(query_str_c_36, "0 1 3|0.2<8722&1.0=2.1&0.2=1.0|1.0", 79);
    bool q_b_36[]={true, true, true, false};
    strncpy(query_str37, "2 1|0.1=1.0&0.1<9795|1.2 0.1", 79);
    strncpy(query_str_c_37, "2 1|0.1<9795&0.1=1.0|1.2 0.1", 79);
    bool q_b_37[]={true, true};
    strncpy(query_str38, "2 0 1|0.2=1.0&1.0=2.2&0.0=9477|0.2", 79);
    strncpy(query_str_c_38, "2 0 1|0.0=9477&1.0=2.2&0.2=1.0|0.2", 79);
    bool q_b_38[]={true, true, true, false};
    strncpy(query_str39, "0 1 2|0.1=1.0&1.0=2.1&0.1<3560|1.2", 79);
    strncpy(query_str_c_39, "0 1 2|0.1<3560&1.0=2.1&0.1=1.0|1.2", 79);
    bool q_b_39[]={true, true, true, false};
    strncpy(query_str40, "2 0 1 3|0.1=1.0&1.0=2.1&1.0=3.1&0.1=141152&2.2=10242743|0.1 2.2 2.1", 79);
    strncpy(query_str_c_40, "2 0 1 3|0.1=141152&2.2=10242743&1.0=3.1&1.0=0.1&1.0=2.1|0.1 2.2 2.1", 79);
    bool q_b_40[]={true, true, false, true, false, true};*/

    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str1, q1), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str2, q2), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str3, q3), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str4, q4), 0);
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str5, q5), 0);
/*    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str6, q6), 0);
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
    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str40, q40), 0);*/
    CU_ASSERT_EQUAL_FATAL(validate_query(q1, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q2, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q3, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q4, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(validate_query(q5, &ti), 0);
/*    CU_ASSERT_EQUAL_FATAL(validate_query(q6, &ti), 0);
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
    CU_ASSERT_EQUAL_FATAL(validate_query(q40, &ti), 0);*/
    CU_ASSERT_EQUAL_FATAL(optimize_query(q1, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q2, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q3, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q4, &ti), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query(q5, &ti), 0);
/*    CU_ASSERT_EQUAL_FATAL(optimize_query(q6, &ti), 0);
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
    CU_ASSERT_EQUAL_FATAL(optimize_query(q40, &ti), 0);*/
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
/*    CU_ASSERT_EQUAL_FATAL(analyze_query(query_str_c_6, q_c_6), 0);
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
    CU_ASSERT_EQUAL_FATAL(validate_query(q_c_40, &ti), 0);*/
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
//    CU_ASSERT_EQUAL_FATAL(compare_bool_arrays(array, q_b_5, 4), true);
    free(array);
/*    array=NULL;
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
    array=NULL;*/
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q1), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q2), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q3), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q4), 0);
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q5), 0);
/*    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q6), 0);
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
    CU_ASSERT_EQUAL_FATAL(optimize_query_memory(q40), 0);*/
//    CU_ASSERT_EQUAL_FATAL(compare_queries(q1, q_c_1), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q2, q_c_2), true);
//    CU_ASSERT_EQUAL_FATAL(compare_queries(q3, q_c_3), true);
    CU_ASSERT_EQUAL_FATAL(compare_queries(q4, q_c_4), true);
//    CU_ASSERT_EQUAL_FATAL(compare_queries(q5, q_c_5), true);
/*    CU_ASSERT_EQUAL_FATAL(compare_queries(q6, q_c_6), true);
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
    CU_ASSERT_EQUAL_FATAL(compare_queries(q40, q_c_40), true);*/

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

void testFilter_original_table()
{
    predicate_filter pf;
    pf.r.table_id=0;
    pf.r.column_id=0;
    pf.filter_type=Greater;
    pf.value=1000;
    table table;
    middle_list* list_greater=create_middle_list();
    if(list_greater==NULL)
    {
        CU_ASSERT(0);
        return;
    }
    middle_list* list_less=create_middle_list();
    if(list_less==NULL)
    {
        CU_ASSERT(0);
        delete_middle_list(list_greater);
        return;
    }
    middle_list* list_equal=create_middle_list();
    if(list_equal==NULL)
    {
        CU_ASSERT(0);
        delete_middle_list(list_greater);
        delete_middle_list(list_less);
        return;
    }
    table.columns=2;
    table.rows=middle_LIST_BUCKET_SIZE*10;
    table.array=NULL;
    //First check the parameters
    CU_ASSERT_NOT_EQUAL(filter_original_table(&pf,&table,list_greater),0);
    table.array=malloc(sizeof(uint64_t*)*table.columns);
    if(table.array==NULL)
    {
        perror("testFilter_original_table: malloc error");
        CU_ASSERT(0);
        delete_middle_list(list_greater);
        delete_middle_list(list_less);
        delete_middle_list(list_equal);
        return;
    }
    for(uint64_t i=0; i<table.columns; i++)
    {
        table.array[i]=malloc(table.rows*sizeof(uint64_t));
        if(table.array[i]==NULL)
        {
            perror("testFilter_original_table: malloc error");
            CU_ASSERT(0);
            for(uint64_t j=0; j<i;j++)
            {
                free(table.array[i]);
            }
            free(table.array);
            delete_middle_list(list_greater);
            delete_middle_list(list_less);
            delete_middle_list(list_equal);
            return;
        }
    }
    CU_ASSERT_NOT_EQUAL(filter_original_table(&pf,&table,NULL),0);
    CU_ASSERT_NOT_EQUAL(filter_original_table(&pf,NULL,list_greater),0);
    CU_ASSERT_NOT_EQUAL(filter_original_table(NULL,&table,list_greater),0);
    for(uint64_t i=0; i<table.rows; i++)
    {
        table.array[0][i]=i%middle_LIST_BUCKET_SIZE;
    }
    CU_ASSERT_EQUAL(filter_original_table(&pf, &table, list_greater),0);
    pf.filter_type=Less;
    CU_ASSERT_EQUAL(filter_original_table(&pf, &table, list_less),0);
    pf.filter_type=Equal;
    CU_ASSERT_EQUAL(filter_original_table(&pf, &table, list_equal),0);
    //Check the results 
    CU_ASSERT_EQUAL(middle_list_get_number_of_records(list_greater),table.rows-(pf.value+1)*10);
    CU_ASSERT_EQUAL(middle_list_get_number_of_records(list_less),(pf.value)*10);
    CU_ASSERT_EQUAL(middle_list_get_number_of_records(list_equal),10);
    middle_list_node* temp=list_greater->head;
    while(temp!=NULL)
    {
        for(unsigned int i=0; i<temp->bucket.index_to_add_next;i++)
        {
            CU_ASSERT_EQUAL(table.array[0][temp->bucket.row_ids[i]]>pf.value,1);
        }
        temp=temp->next;
    }
    temp=list_less->head;
    while(temp!=NULL)
    {
        for(unsigned int i=0; i<temp->bucket.index_to_add_next;i++)
        {
            CU_ASSERT_EQUAL(table.array[0][temp->bucket.row_ids[i]]<pf.value,1);
        }
        temp=temp->next;
    }
    temp=list_equal->head;
    while(temp!=NULL)
    {
        for(unsigned int i=0; i<temp->bucket.index_to_add_next;i++)
        {
            CU_ASSERT_EQUAL(table.array[0][temp->bucket.row_ids[i]]==pf.value,1);
        }
        temp=temp->next;
    }
    delete_middle_list(list_greater);
    delete_middle_list(list_less);
    delete_middle_list(list_equal);
    //Free Table
    for(uint64_t i=0; i<table.columns; i++)
    {
        free(table.array[i]);
    }
    free(table.array);
}

void testFilter_middle_bucket()
{
    predicate_filter pf;
    pf.r.table_id=0;
    pf.r.column_id=0;
    pf.filter_type=Greater;
    pf.value=1000;
    table table;
    middle_list* list_greater=create_middle_list();
    if(list_greater==NULL)
    {
        CU_ASSERT(0);
        return;
    }
    middle_list* list_less=create_middle_list();
    if(list_less==NULL)
    {
        CU_ASSERT(0);
        delete_middle_list(list_greater);
        return;
    }
    middle_list* list_equal=create_middle_list();
    if(list_equal==NULL)
    {
        CU_ASSERT(0);
        delete_middle_list(list_greater);
        delete_middle_list(list_less);
        return;
    }
    table.columns=2;
    table.rows=middle_LIST_BUCKET_SIZE*10;
    table.array=NULL;
    middle_list_bucket bucket;
    bucket.index_to_add_next=0;
    //First check the parameters
    CU_ASSERT_NOT_EQUAL(filter_middle_bucket(&pf,&bucket,&table,list_greater),0);
    table.array=malloc(sizeof(uint64_t*)*table.columns);
    if(table.array==NULL)
    {
        perror("testFilter_middle_bucket: malloc error");
        CU_ASSERT(0);
        delete_middle_list(list_greater);
        delete_middle_list(list_less);
        delete_middle_list(list_equal);
        return;
    }
    for(uint64_t i=0; i<table.columns; i++)
    {
        table.array[i]=malloc(table.rows*sizeof(uint64_t));
        if(table.array[i]==NULL)
        {
            perror("testFilter_middle_bucket: malloc error");
            CU_ASSERT(0);
            for(uint64_t j=0; j<i;j++)
            {
                free(table.array[i]);
            }
            free(table.array);
            delete_middle_list(list_greater);
            delete_middle_list(list_less);
            delete_middle_list(list_equal);
            return;
        }
    }
    CU_ASSERT_NOT_EQUAL(filter_middle_bucket(&pf,&bucket,&table,NULL),0);
    CU_ASSERT_NOT_EQUAL(filter_middle_bucket(&pf,&bucket,NULL,list_greater),0);
    CU_ASSERT_NOT_EQUAL(filter_middle_bucket(&pf,NULL,&table,list_greater),0);
    CU_ASSERT_NOT_EQUAL(filter_middle_bucket(NULL,&bucket,&table,list_greater),0);
    for(uint64_t i=0; i<table.rows; i++)
    {
        table.array[0][i]=i%middle_LIST_BUCKET_SIZE;
    }
    for(uint64_t i=0;i<middle_LIST_BUCKET_SIZE;i++)
    {
        bucket.row_ids[i]=i;
        bucket.index_to_add_next++;
    }
    CU_ASSERT_EQUAL(filter_middle_bucket(&pf, &bucket, &table, list_greater),0);
    pf.filter_type=Less;
    CU_ASSERT_EQUAL(filter_middle_bucket(&pf, &bucket, &table, list_less),0);
    pf.filter_type=Equal;
    CU_ASSERT_EQUAL(filter_middle_bucket(&pf, &bucket, &table, list_equal),0);
    //Check the results 
    CU_ASSERT_EQUAL(middle_list_get_number_of_records(list_greater),middle_LIST_BUCKET_SIZE-(pf.value+1));
    CU_ASSERT_EQUAL(middle_list_get_number_of_records(list_less),(pf.value));
    CU_ASSERT_EQUAL(middle_list_get_number_of_records(list_equal),1);
    middle_list_node* temp=list_greater->head;
    while(temp!=NULL)
    {
        for(unsigned int i=0; i<temp->bucket.index_to_add_next;i++)
        {
            CU_ASSERT_EQUAL(table.array[0][temp->bucket.row_ids[i]]>pf.value,1);
        }
        temp=temp->next;
    }
    temp=list_less->head;
    while(temp!=NULL)
    {
        for(unsigned int i=0; i<temp->bucket.index_to_add_next;i++)
        {
            CU_ASSERT_EQUAL(table.array[0][temp->bucket.row_ids[i]]<pf.value,1);
        }
        temp=temp->next;
    }
    temp=list_equal->head;
    while(temp!=NULL)
    {
        for(unsigned int i=0; i<temp->bucket.index_to_add_next;i++)
        {
            CU_ASSERT_EQUAL(table.array[0][temp->bucket.row_ids[i]]==pf.value,1);
        }
        temp=temp->next;
    }
    delete_middle_list(list_greater);
    delete_middle_list(list_less);
    delete_middle_list(list_equal);
    //Free Table
    for(uint64_t i=0; i<table.columns; i++)
    {
        free(table.array[i]);
    }
    free(table.array);
}

void testSelf_join_table()
{
    predicate_join pj;
    pj.r.table_id=0;
    pj.r.column_id=0;
    pj.s.table_id=0;
    pj.s.column_id=1;
    table table;
    middle_list* self_join_list=create_middle_list();
    if(self_join_list==NULL)
    {
        CU_ASSERT(0);
        return;
    }
    table.columns=2;
    table.rows=middle_LIST_BUCKET_SIZE*10;
    table.array=NULL;
    //First check the parameters
    CU_ASSERT_NOT_EQUAL(self_join_table(&pj,&table,self_join_list),0);
    table.array=malloc(sizeof(uint64_t*)*table.columns);
    if(table.array==NULL)
    {
        perror("testSelf_join_table: malloc error");
        CU_ASSERT(0);
        delete_middle_list(self_join_list);
        return;
    }
    for(uint64_t i=0; i<table.columns; i++)
    {
        table.array[i]=malloc(table.rows*sizeof(uint64_t));
        if(table.array[i]==NULL)
        {
            perror("testSelf_join_table: malloc error");
            CU_ASSERT(0);
            for(uint64_t j=0; j<i;j++)
            {
                free(table.array[i]);
            }
            free(table.array);
            delete_middle_list(self_join_list);
            return;
        }
    }
    CU_ASSERT_NOT_EQUAL(self_join_table(&pj,&table,NULL),0);
    CU_ASSERT_NOT_EQUAL(self_join_table(&pj,NULL,self_join_list),0);
    CU_ASSERT_NOT_EQUAL(self_join_table(NULL,&table,self_join_list),0);
    for(uint64_t i=0; i<table.rows; i++)
    {
        table.array[0][i]=i%middle_LIST_BUCKET_SIZE;
        table.array[1][i]=i+1024%middle_LIST_BUCKET_SIZE;
        if(i%1024==0)
        {
            table.array[1][i]=table.array[0][i];
        }
    }
    CU_ASSERT_EQUAL(self_join_table(&pj, &table, self_join_list),0);
    //Check the results 
    CU_ASSERT_EQUAL(middle_list_get_number_of_records(self_join_list),table.rows/1024);
    middle_list_node* temp=self_join_list->head;
    while(temp!=NULL)
    {
        for(unsigned int i=0; i<temp->bucket.index_to_add_next;i++)
        {
            CU_ASSERT_EQUAL(table.array[0][temp->bucket.row_ids[i]]==table.array[1][temp->bucket.row_ids[i]],1);
        }
        temp=temp->next;
    }
    delete_middle_list(self_join_list);
    //Free Table
    for(uint64_t i=0; i<table.columns; i++)
    {
        free(table.array[i]);
    }
    free(table.array);
}

int original_self_join_middle_bucket(predicate_join* join, middle_list_bucket* bucket, table* table, middle_list* new_list);

void testOriginal_self_join_middle_bucket()
{
    predicate_join pj;
    pj.r.table_id=0;
    pj.r.column_id=0;
    pj.s.table_id=0;
    pj.s.column_id=1;
    table table;
    middle_list* self_join_list=create_middle_list();
    if(self_join_list==NULL)
    {
        CU_ASSERT(0);
        return;
    }
    table.columns=2;
    table.rows=middle_LIST_BUCKET_SIZE*10;
    table.array=NULL;
    middle_list_bucket bucket;
    bucket.index_to_add_next=0;
    //First check the parameters
    CU_ASSERT_NOT_EQUAL(original_self_join_middle_bucket(&pj,&bucket,&table,self_join_list),0);
    table.array=malloc(sizeof(uint64_t*)*table.columns);
    if(table.array==NULL)
    {
        perror("testOriginal_self_join_middle_bucket: malloc error");
        CU_ASSERT(0);
        delete_middle_list(self_join_list);
        return;
    }
    for(uint64_t i=0; i<table.columns; i++)
    {
        table.array[i]=malloc(table.rows*sizeof(uint64_t));
        if(table.array[i]==NULL)
        {
            perror("testOriginal_self_join_middle_bucket: malloc error");
            CU_ASSERT(0);
            for(uint64_t j=0; j<i;j++)
            {
                free(table.array[i]);
            }
            free(table.array);
            delete_middle_list(self_join_list);
            return;
        }
    }
    CU_ASSERT_NOT_EQUAL(original_self_join_middle_bucket(&pj,&bucket,&table,NULL),0);
    CU_ASSERT_NOT_EQUAL(original_self_join_middle_bucket(&pj,&bucket,NULL,self_join_list),0);
    CU_ASSERT_NOT_EQUAL(original_self_join_middle_bucket(&pj,NULL,&table,self_join_list),0);
    CU_ASSERT_NOT_EQUAL(original_self_join_middle_bucket(NULL,&bucket,&table,self_join_list),0);
    for(uint64_t i=0; i<table.rows; i++)
    {
        table.array[0][i]=i%middle_LIST_BUCKET_SIZE;
        table.array[1][i]=i+1024%middle_LIST_BUCKET_SIZE;
        if(i%1024==0)
        {
            table.array[1][i]=table.array[0][i];
        }
    }
    for(uint64_t i=0;i<middle_LIST_BUCKET_SIZE;i++)
    {
        bucket.row_ids[i]=i;
        bucket.index_to_add_next++;
    }
    CU_ASSERT_EQUAL(original_self_join_middle_bucket(&pj,&bucket,&table,self_join_list),0);
    //Check the results 
    CU_ASSERT_EQUAL(middle_list_get_number_of_records(self_join_list),middle_LIST_BUCKET_SIZE/1024);
    middle_list_node* temp=self_join_list->head;
    while(temp!=NULL)
    {
        for(unsigned int i=0; i<temp->bucket.index_to_add_next;i++)
        {
            CU_ASSERT_EQUAL(table.array[0][temp->bucket.row_ids[i]]==table.array[1][temp->bucket.row_ids[i]],1);
        }
        temp=temp->next;
    }
    delete_middle_list(self_join_list);
    //Free Table
    for(uint64_t i=0; i<table.columns; i++)
    {
        free(table.array[i]);
    }
    free(table.array);
}

/*
 * job_fifo.c
 */

void testCreate_job_fifo_node()
{
    job_fifo_node* node=create_job_fifo_node();
    CU_ASSERT_NOT_EQUAL_FATAL(node,NULL);
    CU_ASSERT_EQUAL(node->next,NULL);
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE; i++)
    {
        CU_ASSERT_EQUAL(node->bucket.jobs[i],NULL);
    }
    CU_ASSERT_EQUAL(node->bucket.index_to_add_next,0);
    CU_ASSERT_EQUAL(node->bucket.index_to_remove_next,0);
    free(node);
}
void testCreate_job_fifo()
{
    job_fifo* fifo=create_job_fifo();
    CU_ASSERT_NOT_EQUAL_FATAL(fifo,NULL);
    CU_ASSERT_EQUAL(fifo->head,NULL);
    CU_ASSERT_EQUAL(fifo->append_node,NULL);
    CU_ASSERT_EQUAL(fifo->tail,NULL);
    CU_ASSERT_EQUAL(fifo->number_of_nodes,0);
    CU_ASSERT_EQUAL(fifo->number_of_jobs,0);
    free(fifo);
}
void testAppend_to_job_fifo_bucket()
{
    job_fifo_node* n=create_job_fifo_node();
    job jobs[JOB_FIFO_BUCKET_SIZE+1];
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE; i++)
    {
        CU_ASSERT_EQUAL(append_to_job_fifo_bucket(&n->bucket,&jobs[i]),0);
    }
    CU_ASSERT_EQUAL(append_to_job_fifo_bucket(&n->bucket,&jobs[JOB_FIFO_BUCKET_SIZE]),1);
    
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE; i++)
    {
        CU_ASSERT_EQUAL(n->bucket.jobs[i],&jobs[i]);
    }
    free(n);
}
void testIs_job_fifo_bucket_full()
{
    job_fifo_node* n=create_job_fifo_node();
    job j;
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE; i++)
    {
        CU_ASSERT_EQUAL(is_job_fifo_bucket_full(&n->bucket),false);
        CU_ASSERT_EQUAL(append_to_job_fifo_bucket(&n->bucket,&j),0);
    }
    CU_ASSERT_EQUAL(is_job_fifo_bucket_full(&n->bucket),true);
    free(n);
}
void testAppend_Pop_job_fifo()
{
    job_fifo* fifo=create_job_fifo();
    job jobs[5*JOB_FIFO_BUCKET_SIZE];
    CU_ASSERT_EQUAL(is_job_fifo_empty(fifo),true);
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE*5; i++)
    {
        CU_ASSERT_EQUAL(append_to_job_fifo(fifo,&jobs[i]),0);
        CU_ASSERT_EQUAL(is_job_fifo_empty(fifo),false);
    }
    CU_ASSERT_EQUAL(fifo->number_of_jobs,JOB_FIFO_BUCKET_SIZE*5);
    CU_ASSERT_EQUAL(fifo->number_of_nodes,5);
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE*5; i++)
    {
        CU_ASSERT_EQUAL(is_job_fifo_empty(fifo),false);
        CU_ASSERT_EQUAL(pop_from_job_fifo(fifo),&jobs[i]);
    }
    CU_ASSERT_EQUAL(is_job_fifo_empty(fifo),true);
    CU_ASSERT_EQUAL(fifo->number_of_jobs,0);
    CU_ASSERT_EQUAL(fifo->number_of_nodes,2);
    CU_ASSERT_EQUAL(fifo->head,fifo->append_node);
    CU_ASSERT_NOT_EQUAL(fifo->head,fifo->tail);
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE*3; i++)
    {
        CU_ASSERT_EQUAL(append_to_job_fifo(fifo,&jobs[i]),0);
        CU_ASSERT_EQUAL(is_job_fifo_empty(fifo),false);
    }
    CU_ASSERT_EQUAL(fifo->number_of_jobs,JOB_FIFO_BUCKET_SIZE*3);
    CU_ASSERT_EQUAL(fifo->number_of_nodes,3);
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE*2; i++)
    {
        CU_ASSERT_EQUAL(is_job_fifo_empty(fifo),false);
        CU_ASSERT_EQUAL(pop_from_job_fifo(fifo),&jobs[i]);
    }
    CU_ASSERT_EQUAL(fifo->number_of_jobs,JOB_FIFO_BUCKET_SIZE);
    CU_ASSERT_EQUAL(fifo->number_of_nodes,2);
    CU_ASSERT_EQUAL(fifo->head,fifo->append_node);
    CU_ASSERT_NOT_EQUAL(fifo->head,fifo->tail);
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE*2; i++)
    {
        CU_ASSERT_EQUAL(append_to_job_fifo(fifo,&jobs[i]),0);
        CU_ASSERT_EQUAL(is_job_fifo_empty(fifo),false);
    }
    CU_ASSERT_EQUAL(fifo->number_of_jobs,JOB_FIFO_BUCKET_SIZE*3);
    CU_ASSERT_EQUAL(fifo->number_of_nodes,3);
    CU_ASSERT_EQUAL(fifo->tail,fifo->append_node);
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE; i++)
    {
        CU_ASSERT_EQUAL(pop_from_job_fifo(fifo),&jobs[i+JOB_FIFO_BUCKET_SIZE*2]);
    }
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE*2; i++)
    {
        CU_ASSERT_EQUAL(pop_from_job_fifo(fifo),&jobs[i]);
    }
    CU_ASSERT_EQUAL(is_job_fifo_empty(fifo),true);
    CU_ASSERT_EQUAL(fifo->number_of_jobs,0);
    CU_ASSERT_EQUAL(fifo->number_of_nodes,2);
    CU_ASSERT_EQUAL(fifo->head,fifo->append_node);
    CU_ASSERT_NOT_EQUAL(fifo->head,fifo->tail);
    for(uint32_t i=0; i<JOB_FIFO_BUCKET_SIZE*5; i++)
    {
        CU_ASSERT_EQUAL(append_to_job_fifo(fifo,&jobs[i]),0);
        CU_ASSERT_EQUAL(pop_from_job_fifo(fifo),&jobs[i]);
        CU_ASSERT_EQUAL(is_job_fifo_empty(fifo),true);
        CU_ASSERT_EQUAL(fifo->number_of_jobs,0);
        CU_ASSERT_EQUAL(fifo->number_of_nodes,2);
        CU_ASSERT_EQUAL(fifo->head,fifo->append_node);
    }
    delete_job_fifo(fifo);
}

/*
 * job_scheduler.c
 */

//TODO: create_job_scheduler #ifdef
//TODO: schedule_fast_job
//TODO: store_projection_in_scheduler #ifdef
//TODO: get_job
//TODO: create_query_job
//TODO: run_query_job
//TODO: run_execute_job
//TODO: run_prejoin_job #ifdef
//TODO: create_join_job
//TDOO: create_filter_table_job
//TODO: run_filter_table_job
//TODO: create_filter_middle_job
//TODO: run_filter_middle_job
//TODO: run_original_self_join_table_job
//TODO: run_original_self_join_middle_job
//TODO: run_join_job #ifdef
//TODO: create_presort_job
//TODO: create_sort_job
//TODO: create_projection_job
//TODO: run_presort_job #ifdef
//TODO: run_sort_job
//TODO: run_projection_job #ifdef

/*
 * list_array.c
 */

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
        node->next = NULL;
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
        node->next = NULL;
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
            node->next = NULL;
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
            node->next = NULL;
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
            node->next = NULL;
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

/*
 * projection_list.c
 */

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

int main(void)
{
    CU_pSuite pSuite=NULL;

    /* Initialize the CUnit test registry */
    if(CUE_SUCCESS!=CU_initialize_registry())
    {
        return CU_get_error();
    }

    /* Add a suite to the registry */
    pSuite=CU_add_suite("project_tests", init_suite, clean_suite);
    if(NULL==pSuite)
    {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* Add the tests to the suite */
    if( (NULL==CU_add_test(pSuite, "testCreate_histogram1", testCreate_histogram1))||
        (NULL==CU_add_test(pSuite, "testCreate_histogram2", testCreate_histogram2))||
        (NULL==CU_add_test(pSuite, "testCreate_histogram3", testCreate_histogram3))||
        (NULL==CU_add_test(pSuite, "testCreate_histogram4", testCreate_histogram4))||
        (NULL==CU_add_test(pSuite, "testCreate_histogram5", testCreate_histogram5))||
        (NULL==CU_add_test(pSuite, "testTransform_to_psum1", testTransform_to_psum1))||
        (NULL==CU_add_test(pSuite, "testTransform_to_psum2", testTransform_to_psum2))||
        (NULL==CU_add_test(pSuite, "testCopy_relation", testCopy_relation))||
        (NULL==CU_add_test(pSuite, "testCopy_relation_with_psum1", testCopy_relation_with_psum1))||
        (NULL==CU_add_test(pSuite, "testCopy_relation_with_psum2", testCopy_relation_with_psum2))||
        (NULL==CU_add_test(pSuite, "testCopy_relation_with_psum3", testCopy_relation_with_psum3))||
        (NULL==CU_add_test(pSuite, "testCopy_relation_with_psum4", testCopy_relation_with_psum4))||
        (NULL==CU_add_test(pSuite, "testRadix_sort1", testRadix_sort1))||
        (NULL==CU_add_test(pSuite, "testRadix_sort2", testRadix_sort2))||

    	(NULL==CU_add_test(pSuite, "testCreate_relation_from_table1", testCreate_relation_from_table1))||
    	(NULL==CU_add_test(pSuite, "testCreate_relation_from_table2", testCreate_relation_from_table2))||
    	(NULL==CU_add_test(pSuite, "testCreate_relation_from_table3", testCreate_relation_from_table3))||
    	(NULL==CU_add_test(pSuite, "testCreate_relation_from_table4", testCreate_relation_from_table4))||
        (NULL==CU_add_test(pSuite, "testCreate_relation_from_table5", testCreate_relation_from_table5))||
        (NULL==CU_add_test(pSuite, "testRelation_from_file1", testRelation_from_file1))||
        (NULL==CU_add_test(pSuite, "testRelation_from_file2", testRelation_from_file2))||
        (NULL==CU_add_test(pSuite, "testRelation_from_file3", testRelation_from_file3))||
        (NULL==CU_add_test(pSuite, "testRelation_from_file4", testRelation_from_file4))||
        (NULL==CU_add_test(pSuite, "testRelation_from_file5", testRelation_from_file5))||

        (NULL==CU_add_test(pSuite, "testCreate_queue", testCreate_queue))||
        (NULL==CU_add_test(pSuite, "testIs_empty1", testIs_empty1))||
        (NULL==CU_add_test(pSuite, "testIs_empty2", testIs_empty2))||
        (NULL==CU_add_test(pSuite, "testPush1", testPush1))||
        (NULL==CU_add_test(pSuite, "testPush2", testPush2))||
        (NULL==CU_add_test(pSuite, "testPop1", testPop1))||
        (NULL==CU_add_test(pSuite, "testPop2", testPop2))||

        (NULL==CU_add_test(pSuite, "testSwap", testSwap))||
        (NULL==CU_add_test(pSuite, "testPartition", testPartition))||
        (NULL==CU_add_test(pSuite, "testQuicksort", testQuicksort))||

        (NULL==CU_add_test(pSuite, "testFinal_join1", testFinal_join1))||
        (NULL==CU_add_test(pSuite, "testFinal_join2", testFinal_join2))||
        (NULL==CU_add_test(pSuite, "testFinal_join3", testFinal_join3))||
        (NULL==CU_add_test(pSuite, "testFinal_join4", testFinal_join4))||
        (NULL==CU_add_test(pSuite, "testFinal_join5", testFinal_join5))||

        (NULL==CU_add_test(pSuite, "testCreate_middle_list_node", testCreate_middle_list_node))||
        (NULL==CU_add_test(pSuite, "testIs_middle_list_bucket_full", testIs_middle_list_bucket_full))||
        (NULL==CU_add_test(pSuite, "testAppend_to_middle_bucket", testAppend_to_middle_bucket))||
        (NULL==CU_add_test(pSuite, "testCreate_middle_list", testCreate_middle_list))||
        (NULL==CU_add_test(pSuite, "testConstruct_lookup_table", testConstruct_lookup_table))||
        (NULL==CU_add_test(pSuite, "testAppend_to_middle_list", testAppend_to_middle_list))||
        (NULL==CU_add_test(pSuite, "testIs_middle_list_empty", testIs_middle_list_empty))||
        (NULL==CU_add_test(pSuite, "testMiddle_list_get_number_of_buckets", testMiddle_list_get_number_of_buckets))||
        (NULL==CU_add_test(pSuite, "testMiddle_list_get_number_of_records", testMiddle_list_get_number_of_records))||

        (NULL==CU_add_test(pSuite, "testTable_from_File1", testTable_from_File1))||
        (NULL==CU_add_test(pSuite, "testTable_from_File2", testTable_from_File2))||
        (NULL==CU_add_test(pSuite, "testTable_from_File3", testTable_from_File3))||
        (NULL==CU_add_test(pSuite, "testTable_from_File4", testTable_from_File4))||
        (NULL==CU_add_test(pSuite, "testTable_from_File5", testTable_from_File5))||
        (NULL==CU_add_test(pSuite, "testTable_from_File6", testTable_from_File6))||
        (NULL==CU_add_test(pSuite, "testGet_table1", testGet_table1))||
        (NULL==CU_add_test(pSuite, "testGet_table2", testGet_table2))||
        (NULL==CU_add_test(pSuite, "testGet_table3", testGet_table3))||
        (NULL==CU_add_test(pSuite, "testGet_table4", testGet_table4))||

        (NULL==CU_add_test(pSuite, "testString_list_create", testString_list_create))||
        (NULL==CU_add_test(pSuite, "testString_list_insert1", testString_list_insert1))||
        (NULL==CU_add_test(pSuite, "testString_list_insert2", testString_list_insert2))||
        (NULL==CU_add_test(pSuite, "testString_list_insert3", testString_list_insert3))||
        (NULL==CU_add_test(pSuite, "testString_list_remove1", testString_list_remove1))||
        (NULL==CU_add_test(pSuite, "testString_list_remove2", testString_list_remove2))||
        (NULL==CU_add_test(pSuite, "testString_list_remove3", testString_list_remove3))||
        (NULL==CU_add_test(pSuite, "testString_list_remove4", testString_list_remove4))||

        (NULL==CU_add_test(pSuite, "testInitialize_middleman1", testInitialize_middleman1))||
        (NULL==CU_add_test(pSuite, "testInitialize_middleman2", testInitialize_middleman2))||
        (NULL==CU_add_test(pSuite, "testConstruct_relation_from_table1", testConstruct_relation_from_table1))||
        (NULL==CU_add_test(pSuite, "testConstruct_relation_from_table2", testConstruct_relation_from_table2))||
        (NULL==CU_add_test(pSuite, "testConstruct_relation_from_table3", testConstruct_relation_from_table3))||
        (NULL==CU_add_test(pSuite, "testConstruct_relation_from_table4", testConstruct_relation_from_table4))||
        (NULL==CU_add_test(pSuite, "testConstruct_relation_from_middleman1", testConstruct_relation_from_middleman1))||
        (NULL==CU_add_test(pSuite, "testConstruct_relation_from_middleman2", testConstruct_relation_from_middleman2))||
        (NULL==CU_add_test(pSuite, "testRemove_extra_chars", testRemove_extra_chars))||
        (NULL==CU_add_test(pSuite, "testAnalyze_query1", testAnalyze_query1))||
        (NULL==CU_add_test(pSuite, "testAnalyze_query2", testAnalyze_query2))||
        (NULL==CU_add_test(pSuite, "testAnalyze_query3", testAnalyze_query3))||
        (NULL==CU_add_test(pSuite, "testAnalyze_query4", testAnalyze_query4))||
        (NULL==CU_add_test(pSuite, "testAnalyze_query5", testAnalyze_query5))||
        (NULL==CU_add_test(pSuite, "testMove_predicate", testMove_predicate))||
        (NULL==CU_add_test(pSuite, "testValidate_query1", testValidate_query1))||
        (NULL==CU_add_test(pSuite, "testValidate_query2", testValidate_query2))||
        (NULL==CU_add_test(pSuite, "test_counter_list", test_counter_list))||
        (NULL==CU_add_test(pSuite, "testOptimize_query1", testOptimize_query1))||
        (NULL==CU_add_test(pSuite, "testOptimize_query_memory1", testOptimize_query_memory1))||
        (NULL==CU_add_test(pSuite, "testFilter_original_table", testFilter_original_table))||
        (NULL==CU_add_test(pSuite, "testFilter_middle_bucket", testFilter_middle_bucket))||
        (NULL==CU_add_test(pSuite, "testSelf_join_table", testSelf_join_table))||
        (NULL==CU_add_test(pSuite, "testOriginal_self_join_middle_bucket", testOriginal_self_join_middle_bucket))||

        (NULL==CU_add_test(pSuite, "testCreate_job_fifo_node", testCreate_job_fifo_node))||
        (NULL==CU_add_test(pSuite, "testIs_job_fifo_bucket_full", testIs_job_fifo_bucket_full))||
        (NULL==CU_add_test(pSuite, "testAppend_to_job_fifo_bucket", testAppend_to_job_fifo_bucket))||
        (NULL==CU_add_test(pSuite, "testCreate_job_fifo", testCreate_job_fifo))||
        (NULL==CU_add_test(pSuite, "testAppend_to_job_fifo", testAppend_Pop_job_fifo))||

        (NULL==CU_add_test(pSuite, "testCreate_list_array", testCreate_list_array))||
        (NULL==CU_add_test(pSuite, "testAppend_middle_list", testAppend_middle_list))||
        (NULL==CU_add_test(pSuite, "testMerge_middle_lists", testMerge_middle_lists))||
        (NULL==CU_add_test(pSuite, "testMerge_middle_list", testMerge_middle_list))||
        
        (NULL==CU_add_test(pSuite, "testCreate_projection_node", testCreate_projection_node))||
        (NULL==CU_add_test(pSuite, "testCreate_projection_list", testCreate_projection_list))||
        (NULL==CU_add_test(pSuite, "testAppend_to_projection_list", testAppend_to_projection_list))
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
