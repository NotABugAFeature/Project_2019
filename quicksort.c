#include <stdio.h>
#include <inttypes.h>
#include "quicksort.h"

/* Utility function to swap two elements */
void swap(tuple *a, tuple *b)
{
	tuple t = *a;
	*a = *b;
	*b = t;
}


/**
 * Takes last element as pivot, places it in its correct position
 * and places all smaller elements to its left and all greater to its right
 * @param tuple *source Array to be sorted
 * @param uint64_t low Starging index
 * @param uint64_t high Ending index
 */
uint64_t partition(tuple *source, uint64_t low, uint64_t high)
{
	uint64_t pivot = source[high].key;    //pivot
	uint64_t i = low;  //Index of smaller element

	for(uint64_t j = low; j < high; j++)
	{
		//If current element is smaller than the pivot
		if(source[j].key <= pivot)
		{
			swap(&source[i], &source[j]);
			i++;    //Increment index of smaller element
		}
	}
	swap(&source[i], &source[high]);
	return i;
}
 
/** 
 * QuickSort
 * @param tuple *source Array to be sorted
 * @param uint64_t low Starging index
 * @param uint64_t high Ending index
 */
void quicksort(tuple *source, uint64_t low, uint64_t high)
{
	if(source==NULL)
    	{
        	fprintf(stderr, "%s", "quicksort: NULL parameter\n");
        	return;
   	}
	//printf("QUICK: %" PRId64 " to %" PRId64 "\n", low, high);
	if(low < high)
	{
		//pi is partitioning index, source[p] is now at right place
		uint64_t pi = partition(source, low, high);
		if(pi>0)
        	{
			//Separately sort elements before partition and after partition
			quicksort(source, low, pi - 1);
		}
		quicksort(source, pi + 1, high);
	}
}
