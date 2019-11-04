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
 * @param uint64_t low Starging index
 * @param uint64_t high Ending index
 */
uint64_t partition(tuple *, uint64_t, uint64_t);

/** 
 * QuickSort
 * @param tuple *source Array to be sorted
 * @param uint64_t low Starging index
 * @param uint64_t high Ending index
 */
void quicksort(tuple *, uint64_t, uint64_t);
#endif	// QUICKSORT_H
