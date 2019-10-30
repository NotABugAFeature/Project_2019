#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include "radix_sort.h"

//----Histogram----
/**
 * Histogram creation of a relation r
 * We consider the indexes as valid and we perform no check
 *
 * @param relation r - the table for which we create the histogram
 * @param start_index - the starting index of the relation
 * @param end_index - the ending index is the ending_index - 1
 * @param hist - array already allocated and initialized with 0s
 * @param byte_number - ranges from 1 (most significant left-most byte) to 8 (less significant right-most byte)
 */
void create_histogram(relation r, uint64_t start_index, uint64_t end_index, uint64_t *hist, unsigned short byte_number)
{

    for(uint64_t i=start_index; i<end_index; i++)
    {
        uint64_t key=r.tuples[i].key;
        int position=(key>>((8-byte_number)<<3)) & 0xff;
        hist[position]++;
        //printf("key: %" PRIu64 ", position: %d\n", key, position);
    }
}

/**
 * Psum creation of a relation r based on its histogram
 *
 * @param hist - histogram
 */
void transform_to_psum(uint64_t *hist)
{
    int first=1;
    uint64_t offset=0;

    for(int i=0; i<HIST_SIZE; i++)
    {
        if(hist[i]!=0)
        {
            if(first)
            {
                offset=hist[i];
                hist[i]=0;
                first=0;
            }
            else
            {
                uint64_t temp=offset+hist[i];
                hist[i]=offset;
                offset=temp;
            }
        }
        else
            hist[i]=offset;
    }
}
//----Radix Sort----
void copy_relation(relation* source, relation* target, uint64_t start_index, uint64_t end_index)
{
    for(uint64_t i=start_index; i<end_index; i++)
    {
        target->tuples[i].key=source->tuples[i].key;
        target->tuples[i].row_id=source->tuples[i].row_id;
    }
}
/**
 * Copies a part of the source relation to the target relation with the use of 
 * the cumulative histogram (psum)
 * @param relation* The source relation
 * @param relation* The target relation
 * @param uint64_t The starting index of the relation
 * @param uint64_t The ending index of the relation (it is not included in 
 *                 the transfer)
 * @param uint64_t* The cumulative histogram (psum)
 * @param unsigned short Which byte is used to search in the cumulative histogram
 * @return 
 */
int copy_relation_with_psum(relation* source, relation* target,uint64_t index_start,uint64_t index_end,uint64_t* psum,unsigned short nbyte)
{
    //Check the values given //Can be removed
    if(source==NULL||source->tuples==NULL||target==NULL||target->tuples==NULL||
            index_start>=index_end||index_end>source->num_tuples||
            source->num_tuples!=target->num_tuples)
    {
        fprintf(stderr, "%s", "copy_relation: wrong parameters\n");
        return 1;
    }
    //Start the copy
    uint64_t target_index;
    short histogram_index;
    for(uint64_t i=index_start;i<index_end;i++)
    {
        //Find in which place the next tuple will be copied to from the psum
        histogram_index = ((source->tuples[i].key) >> ((8-nbyte) << 3)) & 0xff;
       // printf("i: %" PRIu64 " histogram %hd\n",i,histogram_index);
        //printf("psum[histogram_index]: %" PRIu64 "\n", psum[histogram_index]);
        target_index=psum[histogram_index]+index_start;
        //printf("%" PRIu64 "\n",target_index); //will be removed 
        //Increase the psum index for the next tuple with the same byte
        psum[histogram_index]++;
        //Copy the tuple
        target->tuples[target_index].key=source->tuples[i].key;
        target->tuples[target_index].row_id=source->tuples[i].row_id;
    }
    return 0;
}

void radix_sort(unsigned short byte, relation *source, relation *result, uint64_t start_index, uint64_t end_index)
{
	printf("Byte:%d RADIX: %" PRIu64 " to %" PRIu64 "\n",byte, start_index, end_index);
	//Check if bucket is small enough
	if((end_index-start_index)*sizeof(tuple)<20||byte>8)
	{
		printf("RADIX TO QUICK: %" PRIu64 " to %" PRIu64 "\n", start_index, end_index-1);
		//Choose whether to place result in source or result array
		if(byte % 2 == 0)
		{
			quicksort(source->tuples, start_index, end_index-1, NULL);
			//copy_relation(source, result, start_index, end_index);
		}
		else
		{
			//quicksort(source->tuples, start_index, end_index-1, result->tuples);
			quicksort(source->tuples, start_index, end_index-1, NULL);
			copy_relation(source, result, start_index, end_index);
		}
	}
	else
	{
		uint64_t *hist = malloc(HIST_SIZE*sizeof(uint64_t));
		for(uint64_t i=0; i<HIST_SIZE; i++)
		{
			hist[i] = 0;
		}
		create_histogram(*source, start_index, end_index, hist, byte);
		printf("histogram ok\n");
	/*	printf("HIST:\n");
		for(uint64_t i=0; i<HIST_SIZE; i++)
		{
			printf("i: %" PRIu64 " - %" PRIu64 "\n", i, hist[i]);
		}*/
		transform_to_psum(hist);
		printf("psum ok\n");
		/*printf("PSUM:\n");
		for(uint64_t i=0; i<HIST_SIZE; i++)
		{
			printf("i: %" PRIu64 " - %" PRIu64 "\n", i, hist[i]);
		}*/
		copy_relation_with_psum(source, result, start_index, end_index, hist, byte);
		printf("relation ok\n");
		/*printf("PSUM THEN:\n");
		for(uint64_t i=0; i<HIST_SIZE; i++)
		{
			printf("i: %" PRIu64 " - %" PRIu64 "\n", i, hist[i]);
		}*/
		//printf("And result: \n");
		//print_relation(result);
		
		//Recursively sort every part of the array
		uint64_t start = start_index;
		for(uint64_t i=0; i<HIST_SIZE; i++)
		{
			//if(start < hist[i])
			if(start<start_index+hist[i])
			{
				//radix_sort(byte+1, result, source, start, hist[i]);
				radix_sort(byte+1, result, source, start, start_index+hist[i]);
			}
			if(hist[i]+start_index>end_index)
            		{
                		break;
            		}
			//start = hist[i];
			start=start_index+hist[i];
		}
		free(hist);
	}
}
