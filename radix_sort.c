#include <stdio.h>
#include <stdlib.h>
#include "GeneralHeader.h"

void copy_array(relation *initial, relation *result, uint64_t start_index, uint64_t end_index)
{
	for(int i=start_index; i<end_index; i++)
	{
		result->tuples[i] = inital->tuples[i];
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
 * @param unsigned Which byte is used to search in the cumulative histogram
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
        histogram_index = ((source->tuples[i].key) >> ((8-nbyte) << 7)) & 0xff;
        printf("i: %ld histogram %hd\n",i,histogram_index);
        target_index=psum[histogram_index]+index_start;
        printf("%ld\n",target_index); //will be removed 
        //Increase the psum index for the next tuple with the same byte
        psum[histogram_index]++;
        //Copy the tuple
        target->tuples[target_index].key=source->tuples[i].key;
        target->tuples[target_index].row_id=source->tuples[i].row_id;
    }
    return 0;
}

void radix_sort(unsigned short byte, relation *initial, relation *result, uint64_t start_index, uint64_t end_index)
{
	if(end_index - start_index < 64KB)  //TODO
	{
		if(byte % 2 == 0)
		{
			quicksort(initial, start_index, end_index);
			copy_array(initial, result, start_index, end_index);
		}
		else
		{
			quicksort(initial, end_index);
		}
	}
	else
	{
		histogram *hist = malloc(sizeof(histogram));
		create_histogram_from_relation(initial, start_index, end_index, byte);
		transform_histogram_to_cumulative_histogram(hist);
		split_and_fill_array(initial, result, start_index, end_index, hist);
		
		uint64_t start = start_index;
		for(int i=0; i<HIST_SIZE; i++)
		{
			radix_sort(byte+1, result, initial, start, hist->array[i]);
			start = hist->array[i];
		}
	}
}
