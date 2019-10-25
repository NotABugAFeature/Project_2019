#ifndef QUICKSORT_H
#define QUICKSORT_H
#include"relation.h"
/* Utility function to swap two elements */
void swap(tuple *, tuple *);

/* This function takes last element as pivot, places 
    the pivot element at its correct position in sorted 
    array, and places all smaller (smaller than pivot) 
   to left of pivot and all greater elements to right 
   of pivot */
/**
 * Takes last element as pivot, places it in its correct position
 * and places all smaller elements to its left and all greater to its right
 * @param tuple *source Array to be sorted
 * @param int64_t low Starging index
 * @param int64_t high Ending index
 * @param tuple *target Array to place results
 */
int partition(tuple *, int64_t, int64_t, tuple *);

/** 
 * QuickSort (sorted array can be found both in source and in target)
 * @param tuple *source Array to be sorted
 * @param int64_t low Starging index
 * @param int64_t high Ending index
 * @param tuple *target Array to place results (can be given as NULL if not needed)
 */
void quicksort(tuple *, int64_t, int64_t, tuple *);
#endif	// QUICKSORT_H
