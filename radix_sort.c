#include <stdio.h>
#include <stdlib.h>
#include "GeneralHeader.h"

void copy_array(relation *initial, relation *result, uint64_t start_index, uint64_t end_index)
{
	for(int i=start_index; i<end_index; i++)
	{
		result.tuples[i] = inital.tuples[i];
	}
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
			radix_sort(byte+1, result, initial, start, hist.array[i]);
			start = hist.array[i];
		}
	}
}