/* Utility function to swap two elements */
void swap(int *, int *);

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
int partition(int *, int, int, int *);

/** 
 * QuickSort (sorted array can be found both in source and in target)
 * @param int *source Array to be sorted
 * @param int low Starging index
 * @param int high Ending index
 * @param int *target Array to place results (can be given as NULL if not needed)
 */
void quicksort(int *, int, int, int *);