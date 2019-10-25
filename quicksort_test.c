#include <stdio.h>
#include <stdlib.h>
#include "quicksort.h"

/* Function to print an Array */
void print_array(int source[], int size)
{
	int i;
	for(i=0; i < size; i++)
	{
		printf("%d ", source[i]);
	}
	printf("\n");
}

// Driver program to test above functions
int main()
{
	int source[] = {3, 10, 7, 4, 8, 9, 1, 5};
	int *target = malloc(8*sizeof(int));
	int n = sizeof(source)/sizeof(source[0]);
	printf("Arra: \n");
	print_array(source, n);
	printf("and \n");
	print_array(target, n);
	quicksort(source, 0, n-1, target);
	printf("Sorted sourceay: \n");
	print_array(source, n);
	printf("and \n");
	print_array(target, n);
	free(target);
	return 0;
}