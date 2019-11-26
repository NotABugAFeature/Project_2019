#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include "radix_sort.h"
#include "queue.h"


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
 * @return 0 if successful else 1
 */
int create_histogram(relation* r, uint64_t start_index, uint64_t end_index, uint64_t *hist, unsigned short byte_number)
{
    if(r == NULL || r->tuples == NULL || hist == NULL)
    {
        fprintf(stderr, "%s", "create_histogram: null parameters\n");
        return 1;
    }

    for(uint64_t i=start_index; i<end_index; i++)
    {
        uint64_t key=r->tuples[i].key;
        int position=(key>>((8-byte_number)<<3)) & 0xff;
        hist[position]++;
    }
    
    return 0;
}


/**
 * Psum creation of a relation r based on its histogram
 *
 * @param hist - histogram
 * @return 0 if successful else 1
 */
int transform_to_psum(uint64_t *hist)
{
    if(hist == NULL)
    {
        fprintf(stderr, "%s", "transform_to_psum: null parameter\n");
        return 1;
    }

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
    
    return 0;
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
        target_index=psum[histogram_index]+index_start;

        //Increase the psum index for the next tuple with the same byte
        psum[histogram_index]++;
        //Copy the tuple
        target->tuples[target_index].key=source->tuples[i].key;
        target->tuples[target_index].row_id=source->tuples[i].row_id;
    }
    return 0;
}


/**
 * Implements radix sort
 * in bfs order, using a queue
 * @param relation *array The array to be sorted
 * @return 0 for success, <0 for error
 */
int radix_sort(relation *array)
{
	//Create array to help with the sorting
	relation *auxiliary = malloc(sizeof(relation));
	if(auxiliary == NULL)
	{
		perror("radix_sort: malloc error");
		return -1;
	}
	auxiliary->num_tuples = array->num_tuples;
	auxiliary->tuples = malloc(array->num_tuples*sizeof(tuple));
	if(auxiliary->tuples == NULL)
	{
		perror("radix_sort: malloc error");
		return -1;
	}

	window *win, *new_win;
	queue *q = create_queue();
	win = malloc(sizeof(window));
	win->start = 0;
	win->end = array->num_tuples;
	win->byte = 1;
	push(q, win);
	
	while(!is_empty(q))
	{
		new_win = pop(q);

		//Change of level, swap relations
		if(new_win->byte > win->byte)
		{
			relation *temp = array;
			array = auxiliary;
			auxiliary = temp;
		}
		win = new_win;
		if((win->end - win->start)*sizeof(tuple) > 64*1024 || win->byte>8)
		{
			//Choose whether to place result in array or auxiliary array
			if(win->byte % 2 == 0)
			{
				quicksort(array->tuples, win->start, win->end-1);
				copy_relation(array, auxiliary, win->start, win->end);
			}
			else
			{
				quicksort(array->tuples, win->start, win->end-1);
			}
		}
		else
		{
			uint64_t *hist = malloc(HIST_SIZE*sizeof(uint64_t));
			if(hist == NULL)
			{
				perror("radix_sort_recursive: malloc error");
				return -2;
			}
			
			for(uint64_t i=0; i<HIST_SIZE; i++)
			{
				hist[i] = 0;
			}
			
			int res = create_histogram(array, win->start, win->end, hist, win->byte);
			if(res)
				return -3;
			
			res = transform_to_psum(hist);
			if(res)
				return -4;
			
			copy_relation_with_psum(array, auxiliary, win->start, win->end, hist, win->byte);
			
			//Recursively sort every part of the array
			uint64_t start = win->start;
			for(uint64_t i=0; i<HIST_SIZE; i++)
			{
				if(start<win->start+hist[i])
				{
					//int retval = radix_sort_recursive(byte+1, auxiliary, array, start, start_index+hist[i]);
					new_win = malloc(sizeof(window));
					new_win->start = start;
					new_win->end = win->start+hist[i];
					new_win->byte = win->byte + 1;
					push(q, new_win);
				}
				if(hist[i]+win->start>win->end)
				{
					break;
				}
				start=win->start+hist[i];
			}
			free(hist);
		}
		
	}
	
	free(q);

	free(auxiliary->tuples);
	free(auxiliary);
	return 0;
}
