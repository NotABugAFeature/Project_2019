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
#include "./result_list/result_list.h"
#include "relation.h"
//#include "GeneralHeader.h"

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
    //Relation Creation Testing
    uint64_t size=16;
    uint64_t* table=malloc(sizeof(uint64_t)*size);
    for(uint64_t i=0;i<size;i++)
    {
        if(i%2==0)
        {
            table[i]=(size*1024-i);
        }
        else
        {
            table[i]=(size-i-1);
        }
    }
    relation r;r.tuples=NULL;r.num_tuples=0;
    create_relation_from_table(table,size,&r);
    relation r_c;
    r_c.num_tuples=size;
    free(table);
    r_c.tuples=malloc(r_c.num_tuples*sizeof(tuple));
    for(int i=0;i<r_c.num_tuples;i++)
    {
        r_c.tuples[i].key=0;
    }
    //Sort
    radix_sort(1,&r,&r_c,0,size-1);
    printf("RESULT\n\n\n");
    print_relation(&r);
    print_relation(&r_c);
    int correct=1;
    for(int64_t i=0;i<size-1;i++)
    {
        if(r.tuples[i].key>r.tuples[i+1].key)
        {
            printf("Error i: %ld\t%ld\t%ld\n",i,r.tuples[i].key,r.tuples[i+1].key);
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
