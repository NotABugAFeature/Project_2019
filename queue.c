#include <stdio.h>
#include <stdlib.h>
#include "queue.h"


queue *create_queue()
{
	queue *q = malloc(sizeof(queue));
	q->head = NULL;
	q->tail = NULL;
	return q;
}

bool is_empty(queue *q)
{
	return q->head == NULL;
}

void push(queue *q, window *rec)
{
	q_node *new_node = malloc(sizeof(q_node));
	new_node->record = rec;
	new_node->next = NULL;
	if(q->head == NULL || q->tail == NULL)
	{
		q->head = new_node;
	}
	else
	{
		q->tail->next = new_node;
	}
	q->tail = new_node;
}


window *pop(queue *q)
{
	if(q->head == NULL)
	{
		return NULL;
	}

	q_node *first_node = q->head;
	window *r = first_node->record;
	q->head = q->head->next;
	
	if(q->head == NULL)
	{
		q->tail = NULL;
	}
	free(first_node);
	
	return r;
}