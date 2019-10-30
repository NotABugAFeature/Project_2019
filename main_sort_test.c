/******************************************************************************/
/*  Application         :    Project_2019                                     */
/*  File                :    main.c                                           */
/*  Team Member         :    Georgakopoulos Panagiotis 1115201600028          */
/*  Team Member         :    Karamhna Maria            1115201600059          */
/*  Team Member         :    Koursiounis Georgios      1115201600077          */
/*  Instructor          :    Sarantis Paskalis                                */
/*  All Tests Conducted At The University's Linux Machines                    */
/******************************************************************************/
/*  Main function for testing                                                 */

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include "./result_list/result_list.h"
#include "radix_sort.h"

int main(int argc, char** argv)
{
    //List Testing
//    result_list* testlist=create_result_list();
//    printf("\n");
//    print_result_list(testlist);
//    uint64_t i=0;
//    
//    printf("%d\n",RESULT_LIST_BUCKET_SIZE);
//    while(i<1048576)
//    {
//        append_to_list(testlist,i,i+1);
//        i++;
//    }
//    print_result_list(testlist);
//    delete_result_list(testlist);
    //Input From File Testing
    table *test_table=read_from_file("./input.txt");
    //Relation Creation Testing
    relation r;r.tuples=NULL;r.num_tuples=0;
    create_relation_from_table(&test_table->array[0][0],test_table->columns,&r);
    print_relation(&r);
    relation r_c;
    r_c.num_tuples=r.num_tuples;
    for(uint64_t i=0; i<test_table->rows; i++)
    {
        free(test_table->array[i]);
    }
    free(test_table->array);
    free(test_table);
    r_c.tuples=malloc(r_c.num_tuples*sizeof(tuple));
    for(int i=0;i<r_c.num_tuples;i++)
    {
        r_c.tuples[i].key=0;
    }
    //Sort
    radix_sort(1,&r,&r_c,0,r.num_tuples);
    printf("RESULT\n\n\n");
    printf("\nR:\n");
    print_relation(&r);
    printf("\nRC:\n");
    print_relation(&r_c);
    int correct=1;
    for(int64_t i=0;i<r_c.num_tuples-1;i++)
    {
        if(r_c.tuples[i].key>r_c.tuples[i+1].key)
        {
            printf("Error i: %" PRId64 "\t%" PRIu64 "\t%" PRIu64 "\n",i,r_c.tuples[i].key,r_c.tuples[i+1].key);
            correct=0;
            //break;
        }
    }
    if(correct)
    {
        printf("Correct\n");
    }
    free(r.tuples);
    free(r_c.tuples);
    return(EXIT_SUCCESS);
}
