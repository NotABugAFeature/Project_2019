#include <stdio.h>
#include "quicksort.h"

/* Utility function to swap two elements */
void swap(int *a, int *b)	//TODO: Maybe should be void *
{
	int t = *a;
	*a = *b;
	*b = t;
}


/* This function takes last element as pivot, places 
    the pivot element at its correct position in sorted 
    array, and places all smaller (smaller than pivot) 
   to left of pivot and all greater elements to right 
   of pivot */
/**
 * Takes last element as pivot, places it in its correct position
 * and places all smaller elements to its left and all greater to its right
 * @param int *source Array to be sorted
 * @param int low Starging index
 * @param int high Ending index
 * @param int *target Array to place results
 */
int partition(int *source, int low, int high, int *target)
{
	int pivot = source[high];    //pivot
	int i = (low - 1);  //Index of smaller element

	for(int j = low; j <= high- 1; j++)
	{
		//If current element is smaller than the pivot
		if(source[j] < pivot)
		{
			i++;    //Increment index of smaller element
			if(target != NULL && target != source)
			{
				target[i] = source[j];
				target[j] = source[i];
			}
			swap(&source[i], &source[j]);
		}
	}
	if(target != NULL && target != source)
	{
		target[i + 1] = source[high];
		target[high] = source[i + 1];
	}
	swap(&source[i + 1], &source[high]);
	return (i + 1);
}
 
/** 
 * QuickSort (sorted array can be found both in source and in target)
 * @param int *source Array to be sorted
 * @param int low Starging index
 * @param int high Ending index
 * @param int *target Array to place results (can be given as NULL if not needed)
 */
void quicksort(int *source, int low, int high, int *target)
{
	if(low < high)
	{
		//pi is partitioning index, source[p] is now at right place
		int pi = partition(source, low, high, target);

		//Separately sort elements before partition and after partition
		quicksort(source, low, pi - 1, target);
		quicksort(source, pi + 1, high, target);
	}
}
