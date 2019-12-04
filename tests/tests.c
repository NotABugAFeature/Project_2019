#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <CUnit/CUnit.h>
#include <CUnit/Basic.h>
#include "../radix_sort.h"
#include "../sort_merge_join.h"
#include "../queue.h"
#include "../result_list.h"
#include "../table.h"
#include "../string_list.h"

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

    p2.num_tuples = 50000000;
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
    if(t != NULL)
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
	delete_table(t);
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
	delete_table(t);
}

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
 * quicksort_test.c
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
  * Test case 1. Relation t is NULL
  *
  */
 void testFinal_join1()
 {
     result_list* list = create_result_list();
     relation p2;
     int res = final_join(list, NULL, &p2);

     CU_ASSERT_EQUAL(res, 1);
     delete_result_list(list);
 }


 /**
  * Test case 2. Relation s is NULL
  *
  */
 void testFinal_join2()
 {
     result_list* list = create_result_list();
     relation p2;
     int res = final_join(list, &p2, NULL);

     CU_ASSERT_EQUAL(res, 1);
     delete_result_list(list);
 }


 /**
  * Test case 3. Result list is NULL
  *
  */
 void testFinal_join3()
 {
     relation p1, p2;
     int res = final_join(NULL, &p1, &p2);

     CU_ASSERT_EQUAL(res, 1);
 }


 /**
  * Test case 4. Relation t is empty
  *
  */
 void testFinal_join4()
 {
     result_list* list = create_result_list();

     relation p1, p2;

     p1.num_tuples = 0;
     p1.tuples = NULL;

     p2.num_tuples = 3;
     p2.tuples = malloc(3*sizeof(tuple));
     p2.tuples[0].key = 1;
     p2.tuples[0].row_id = 1;

     p2.tuples[1].key = 1;
     p2.tuples[1].row_id = 2;

     p2.tuples[2].key = 3;
     p2.tuples[2].row_id = 3;


     int res = final_join(list, &p1, &p2);

     CU_ASSERT_EQUAL(res, 1);

     free(p2.tuples);
     delete_result_list(list);
 }


 /**
  * Test case 5. Relation s is empty
  *
  */
 void testFinal_join5()
 {
     result_list* list = create_result_list();

     relation p1, p2;

     p1.num_tuples = 0;
     p1.tuples = NULL;

     p2.num_tuples = 3;
     p2.tuples = malloc(3*sizeof(tuple));
     p2.tuples[0].key = 1;
     p2.tuples[0].row_id = 1;

     p2.tuples[1].key = 1;
     p2.tuples[1].row_id = 2;

     p2.tuples[2].key = 3;
     p2.tuples[2].row_id = 3;


     int res = final_join(list, &p2, &p1);

     CU_ASSERT_EQUAL(res, 1);

     free(p2.tuples);
     delete_result_list(list);
 }


 /**
  * Test case 6. Successful test
  *
  */
 void testFinal_join6()
 {
     result_list* list = create_result_list();

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


     int res = final_join(list, &p1, &p2);

     CU_ASSERT_EQUAL(res, 0);
     CU_ASSERT_EQUAL(result_list_get_number_of_records(list), 3);

     free(p1.tuples);
     free(p2.tuples);
     delete_result_list(list);
 }


 /**
  * Test case 7. Successful test
  *
  */
 void testFinal_join7()
 {
     result_list* list = create_result_list();

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


     int res = final_join(list, &p1, &p2);

     CU_ASSERT_EQUAL(res, 0);
     CU_ASSERT_EQUAL(result_list_get_number_of_records(list), 0);

     free(p1.tuples);
     free(p2.tuples);
     delete_result_list(list);
 }


 /**
  * Test case 8. Successful test
  *
  */
 void testFinal_join8()
 {
     result_list* list = create_result_list();

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


     int res = final_join(list, &p1, &p2);

     CU_ASSERT_EQUAL(res, 0);
     CU_ASSERT_EQUAL(result_list_get_number_of_records(list), 2);

     free(p1.tuples);
     free(p2.tuples);
     delete_result_list(list);
 }


 /**
  * Test case 1. relR is NULL
  *
  */
 void testSort_merge_join1()
 {
     relation relS;
     result_list* result = sort_merge_join(NULL, &relS);
     CU_ASSERT_EQUAL(result, NULL);
 }


 /**
  * Test case 2. relS is NULL
  *
  */
 void testSort_merge_join2()
 {
     relation relR;
     result_list* result = sort_merge_join(&relR, NULL);
     CU_ASSERT_EQUAL(result, NULL);
 }


 /**
  * Test case 3. Successful test with small relations
  *
  */
 void testSort_merge_join3()
 {
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

     result_list* result = sort_merge_join(&p1, &p2);

     CU_ASSERT_NOT_EQUAL(result, NULL);
     CU_ASSERT_EQUAL(result_list_get_number_of_records(result), 3);

     free(p1.tuples);
     free(p2.tuples);
     delete_result_list(result);
 }


 /**
  * Test case 4. Successful test with one small and one big (ordered) relation
  *
  */
 void testSort_merge_join4()
 {
     relation p1, p2;

     p1.num_tuples = 100000;
     uint64_t init = 2251799813685020;
     p1.tuples = malloc((p1.num_tuples)*sizeof(tuple));
     for(int i = 0; i < p1.num_tuples; i++)
     {
         p1.tuples[i].key = init;
         init++;
         p1.tuples[i].row_id = i;
     }

     p2.num_tuples = 3;
     p2.tuples = malloc(3*sizeof(tuple));
     p2.tuples[0].key = 2251799813685081;
     p2.tuples[0].row_id = 1;

     p2.tuples[1].key = 2251799813685127;
     p2.tuples[1].row_id = 2;

     p2.tuples[2].key = 2251799813685020;
     p2.tuples[2].row_id = 3;

     result_list* result = sort_merge_join(&p1, &p2);

     CU_ASSERT_NOT_EQUAL(result, NULL);
     CU_ASSERT_EQUAL(result_list_get_number_of_records(result), 3);

     free(p1.tuples);
     free(p2.tuples);
     delete_result_list(result);
 }


 /**
  * Test case 5. Successful test with one small and one big (reverse order) relation
  *
  */
 void testSort_merge_join5()
 {
     relation p1, p2;

     p1.num_tuples = 300000;
     uint64_t init = 4647856104846919648;
     p1.tuples = malloc((p1.num_tuples)*sizeof(tuple));
     for(int i = 0; i < p1.num_tuples; i++)
     {
         p1.tuples[i].key = init;
         init--;
         p1.tuples[i].row_id = i;
     }

     p2.num_tuples = 3;
     p2.tuples = malloc(3*sizeof(tuple));
     p2.tuples[0].key = 4647856104846919348;
     p2.tuples[0].row_id = 1;

     p2.tuples[1].key = 4647856104846919148;
     p2.tuples[1].row_id = 2;

     p2.tuples[2].key = 4647856104846911248;
     p2.tuples[2].row_id = 3;

     result_list* result = sort_merge_join(&p1, &p2);

     CU_ASSERT_NOT_EQUAL(result, NULL);
     CU_ASSERT_EQUAL(result_list_get_number_of_records(result), 3);

     free(p1.tuples);
     free(p2.tuples);
     delete_result_list(result);
 }


 /**
  * Test case 6. Successful test with two big relations
  *
  */
 void testSort_merge_join6()
 {
     relation p1, p2;

     p1.num_tuples = 100000;
     uint64_t init = 4647856104846919648;
     p1.tuples = malloc((p1.num_tuples)*sizeof(tuple));
     for(int i = 0; i < p1.num_tuples; i++)
     {
         p1.tuples[i].key = init;
         init--;
         p1.tuples[i].row_id = i;
     }

     p2.num_tuples = 100000;
     init=4647856104846919648;
     p2.tuples = malloc((p2.num_tuples)*sizeof(tuple));
     for(int i = 0; i < p2.num_tuples; i++)
     {
         p2.tuples[i].key = init;
         init--;
         p2.tuples[i].row_id = i;
     }

     result_list* result = sort_merge_join(&p1, &p2);

     CU_ASSERT_NOT_EQUAL(result, NULL);
     CU_ASSERT_EQUAL(result_list_get_number_of_records(result), 100000);

     free(p1.tuples);
     free(p2.tuples);
     delete_result_list(result);
 }


/*
 *
 */

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
 void print_result_list(result_list*,FILE*);
 void print_bucket(result_list_bucket*,FILE*);
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
     //print_bucket(&bucket,stdout);
 }

 void testCreate_result_list()
 {
     //Create a result list
     result_list* list=create_result_list();
     //Check if the list is initialized correctly
     if(list==NULL||list->head!=NULL||list->number_of_nodes!=0||list->tail!=NULL)
     {
         CU_ASSERT(0);
     }
     free(list);
 }

 void testAppend_to_list()
 {
     //Create a result list
     result_list* list=create_result_list();
     if(list==NULL)//Check if it was created
     {
         CU_ASSERT(0);
         return;
     }
     uint64_t r_row_id=1;
     uint64_t s_row_id=2;
     unsigned int i=0;
     //Append the maximum records of one node in the list
     for(;i<RESULT_LIST_BUCKET_SIZE;i++)
     {
         //Check if the items were added correctly in head and tail nodes
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
     //Check if only one node exists and it is full
     if(list->number_of_nodes!=1||is_result_list_bucket_full(&list->head->bucket)!=1||list->head->bucket.index_to_add_next!=i||list->head!=list->tail)
     {
         CU_ASSERT(0);
     }
     //Append the maximum records of one node in the list
     for(i=0;i<RESULT_LIST_BUCKET_SIZE;i++)
     {
         //Check if the items were added correctly in tail node
         if(append_to_list(list, r_row_id, s_row_id)!=0||list->tail->bucket.row_ids[i][ROWID_R_INDEX]!=r_row_id||list->tail->bucket.row_ids[i][ROWID_S_INDEX]!=s_row_id)
         {
             CU_ASSERT(0);
         }
         r_row_id+=2;
         s_row_id+=2;
     }
     //Check if two nodes exists and are both full
     if(list->number_of_nodes!=2||is_result_list_bucket_full(&list->head->bucket)!=1||is_result_list_bucket_full(&list->tail->bucket)!=1||list->head->bucket.index_to_add_next!=i||list->tail->bucket.index_to_add_next!=i||list->head==list->tail)
     {
         CU_ASSERT(0);
     }
     //print_result_list(list,stdout);
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
     //print_result_list(list,stdout);
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
     //print_result_list(list,stdout);
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
     //print_result_list(list,stdout);
     delete_result_list(list);
 }

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
	for(int i=0; i<columns; i++)
	{
		for(int j=0; j<rows; j++)
		{
			CU_ASSERT_EQUAL(t->array[i][j], i+j);
		}
	}
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
	for(int i=0; i<columns; i++)
	{
		for(int j=0; j<rows; j++)
		{
			CU_ASSERT_EQUAL(t->array[i][j], i+j);
		}
	}
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
	for(int i=0; i<columns; i++)
	{
		for(int j=0; j<rows; j++)
		{
			CU_ASSERT_EQUAL(t->array[i][j], i+j);
		}
	}
}

/*
 * Test case 1: Table index is NULL
 */
void testGet_table1()
{
	table_index *ti = NULL;
	table *t = get_table(ti, 1);
	CU_ASSERT_EQUAL(t, NULL);
}

/*
 * Test case 2: Tables of table index is NULL
 */
void testGet_table2()
{
	table_index *ti = malloc(sizeof(table_index));
	ti->tables = NULL;
	table *t = get_table(ti, 1);
	CU_ASSERT_EQUAL(t, NULL);
}

/*
 * Test case 3: Table does not exist
 */
void testGet_table3()
{
	table_index *ti = malloc(sizeof(table_index));
	ti->num_tables = 5;
	ti->tables = malloc(ti->num_tables*sizeof(table));
	for(int i=0; i<ti->num_tables; i++)
	{
		ti->tables[i].table_id = i;
	}

	table *t = get_table(ti, 10);
	CU_ASSERT_EQUAL(t, NULL);
}

/*
 * Test case 3: Table exists
 */
void testGet_table4()
{
	table_index *ti = malloc(sizeof(table_index));
	ti->num_tables = 100;
	ti->tables = malloc(ti->num_tables*sizeof(table));
	for(int i=0; i<ti->num_tables; i++)
	{
		ti->tables[i].table_id = i;
	}

	table *t = get_table(ti, 10);
	CU_ASSERT_EQUAL(t->table_id, 10);
}


void testString_list_create()
{
    string_list *list = string_list_create();
    CU_ASSERT_EQUAL(list->num_nodes, 0);
    CU_ASSERT_EQUAL(list->head, NULL);
    CU_ASSERT_EQUAL(list->tail, NULL);
    free(list);
}

/*
 * Test case 1: list is NULL
 */
void testString_list_insert1()
{
    string_list *list = NULL;
    int res = string_list_insert(list, "string1");
    CU_ASSERT_NOT_EQUAL(res, 0);
}

/*
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

/*
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

/*
 * Test case 1: list is NULL
 */
void testString_list_remove1()
{
    string_list *list = NULL;
    char *str = string_list_remove(list);
    CU_ASSERT_EQUAL(str, NULL);
}

/*
 * Test case 2: list is empty
 */
void testString_list_remove2()
{
    string_list *list = string_list_create();
    char *str = string_list_remove(list);
    CU_ASSERT_EQUAL(str, NULL);

    free(list);
}

/*
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

/*
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
    }

    free(list);
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
    pSuite=CU_add_suite("radix_sort_test", init_suite, clean_suite);
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

     //   (NULL==CU_add_test(pSuite, "testRead_from_file1", testRead_from_file1))||
    //		(NULL==CU_add_test(pSuite, "testRead_from_file2", testRead_from_file2))||
    //		(NULL==CU_add_test(pSuite, "testRead_from_file3", testRead_from_file3))||
    //		(NULL==CU_add_test(pSuite, "testRead_from_file4", testRead_from_file4))||
    //		(NULL==CU_add_test(pSuite, "testRead_from_file5", testRead_from_file5))||
    //		(NULL==CU_add_test(pSuite, "testRead_from_file6", testRead_from_file6))||
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
        (NULL==CU_add_test(pSuite, "testFinal_join6", testFinal_join6))||
        (NULL==CU_add_test(pSuite, "testFinal_join7", testFinal_join7))||
        (NULL==CU_add_test(pSuite, "testFinal_join8", testFinal_join8))||
        (NULL==CU_add_test(pSuite, "testSort_merge_join1", testSort_merge_join1))||
        (NULL==CU_add_test(pSuite, "testSort_merge_join2", testSort_merge_join2))||
        (NULL==CU_add_test(pSuite, "testSort_merge_join3", testSort_merge_join3))||
        (NULL==CU_add_test(pSuite, "testSort_merge_join4", testSort_merge_join4))||
        (NULL==CU_add_test(pSuite, "testSort_merge_join5", testSort_merge_join5))||
        (NULL==CU_add_test(pSuite, "testSort_merge_join6", testSort_merge_join6))||

        (NULL==CU_add_test(pSuite, "testCreate_result_list_node", testCreate_result_list_node))||
        (NULL==CU_add_test(pSuite, "testIs_result_list_bucket_full", testIs_result_list_bucket_full))||
        (NULL==CU_add_test(pSuite, "testAppend_to_bucket", testAppend_to_bucket))||
        (NULL==CU_add_test(pSuite, "testCreate_result_list", testCreate_result_list))||
        (NULL==CU_add_test(pSuite, "testAppend_to_list", testAppend_to_list))||
        (NULL==CU_add_test(pSuite, "testIs_result_list_empty", testIs_result_list_empty))||
        (NULL==CU_add_test(pSuite, "testResult_list_get_number_of_buckets", testResult_list_get_number_of_buckets))||
        (NULL==CU_add_test(pSuite, "testResult_list_get_number_of_records", testResult_list_get_number_of_records))||

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
        (NULL==CU_add_test(pSuite, "testString_list_remove4", testString_list_remove4))

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
